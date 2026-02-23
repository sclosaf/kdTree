#include <stdlib.h>
#include <omp.h>

#include "kdTree/kdTree.h"
#include "kdTree/kdTreeBuild.h"
#include "utils/constants.h"
#include "utils/types.h"

static KDNode* buildTreeParallel(point** points, size_t size, int depth);
static Bucket* sievePoints(point** points, size_t size, KDNode* sketch);
static void buildSketch(KDNode** root, point** samples, size_t sampleCount, int level);
static u32 getBucket(KDNode* sketch, point* p);
static KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, int depth);
static f32 findMedian(point** points, size_t start, size_t end, int dim);
static int findSplitDim(point** points, size_t start, size_t end);
static size_t parallelPartition(point** points, size_t start, size_t end, int dim, f32 pivot);
static KDNode* createLeafNode(point** points, size_t size);
static void freeLeafNode(KDNode* node);
static void attachSubtree(KDNode* sketch, int bucketId, KDNode* subtree);
static int compareByDim(const void* a, const void* b, void* dim);
static u32** computePrefixSum(u32** matrix, size_t rows, size_t cols);

// fix ints, manage malloc and **

KDTree* onChipBuild(point** points, size_t size)
{
    if(!points || size == 0)
        return NULL;

    KDTree* tree = (KDTree*)malloc(sizeof(KDTree));
    if(!tree)
        return NULL;

    tree->totalPoints = size;
    tree->totalNodes = 0;

    if(size < CHUNK_SIZE * OVERSAMPLING_RATE)
        tree->root = buildTreeParallelPlain(points, 0, size - 1, 0);
    else
        tree->root = buildTreeParallel(points, size, 0);

    // TO COMPLETE logStar ....

    return tree;
}

static KDNode* buildTreeParallel(point** points, size_t size, int depth)
{
    if(size <= LEAF_WRAP_THRESHOLD)
        return createLeafNode(points, size);

    size_t sampleCount = CHUNK_SIZE * OVERSAMPLING_RATE;
    point** samples = (point**)malloc(sampleCount * sizeof(point*));

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; ++i)
    {
        int index = rand() % size;
        samples[i] = points[index];
    }

    KDNode* sketch = NULL;
    buildSketch(&sketch, samples, sampleCount, SKETCH_HEIGHT);
    free(samples);

    Bucket* buckets = sievePoints(points, size, sketch);

    #pragma omp parallel for
    for(size_t i = 0; i < CHUNK_SIZE; ++i)
    {
        if(buckets[i].size > 0)
        {
            KDNode* subtree = buildTreeParallel(buckets[i].bucket, buckets[i].size, depth + SKETCH_HEIGHT);
            attachSubtree(sketch, i, subtree);
        }
    }

    free(buckets);
    return sketch;
}

static Bucket* sievePoints(point** points, size_t size, KDNode* sketch)
{
    size_t numChunks = (size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    u32** countMatrix = (u32**)malloc(numChunks * sizeof(u32*));

    #pragma omp parallel for
    for(size_t i = 0; i < numChunks; ++i)
    {
        countMatrix[i] = (u32*)calloc(CHUNK_SIZE, sizeof(u32));

        size_t chunkStart = i * CHUNK_SIZE;
        size_t chunkEnd = (chunkStart + CHUNK_SIZE < size) ? (chunkStart + CHUNK_SIZE) : size;

        for(size_t j = chunkStart; j < chunkEnd; ++j)
        {
            u32 bucketId = getBucket(sketch, points[j]);
            countMatrix[i][bucketId]++;
        }
    }

    u32** offsetMatrix = computePrefixSum(countMatrix, numChunks, CHUNK_SIZE);

    u32* bucketOffsets = (u32*)malloc(CHUNK_SIZE * sizeof(u32));
    for(size_t j = 0; j < CHUNK_SIZE; ++j)
        bucketOffsets[j] = offsetMatrix[0][j];

    point** sortedPoints = (point**)malloc(size * sizeof(point*));

    #pragma omp parallel for
    for(size_t i = 0; i < numChunks; ++i)
    {
        size_t chunkStart = i * CHUNK_SIZE;
        size_t chunkEnd = (chunkStart + CHUNK_SIZE < size) ? (chunkStart + CHUNK_SIZE) : size;

        for(size_t j = chunkStart; j < chunkEnd; ++j)
        {
            u32 bucketId = getBucket(sketch, points[j]);
            u32 index = offsetMatrix[i][bucketId];
            sortedPoints[index] = points[j];
            offsetMatrix[i][bucketId]++;
        }
    }

    memcpy(points, sortedPoints, size * sizeof(point*));

    Bucket* buckets = (Bucket*)malloc(CHUNK_SIZE * sizeof(Bucket));

    for(size_t j = 0; j < CHUNK_SIZE; ++j)
    {
        size_t start = bucketOffsets[j];
        size_t end = (j < CHUNK_SIZE - 1) ? bucketOffsets[j + 1] : size;

        buckets[j].bucket = &points[start];
        buckets[j].size = end - start;
    }

    free(sortedPoints);
    free(bucketOffsets);

    for(size_t i = 0; i < numChunks; ++i)
    {
        free(countMatrix[i]);
        free(offsetMatrix[i]);
    }

    free(countMatrix);
    free(offsetMatrix);

    return buckets;
}

static void buildSketch(KDNode** root, point** samples, size_t sampleCount, int levels)
{
    if(levels == 0 || sampleCount <= LEAF_WRAP_THRESHOLD)
    {
        *root = (KDNode*)malloc(sizeof(KDNode));
        if(!*root)
            return;

        (*root)->type = LEAF;
        (*root)->parent = NULL;
        (*root)->data.leaf.points = NULL;
        (*root)->data.leaf.pointCount = 0;

        return;
    }

    *root = (KDNode*)malloc(sizeof(KDNode));
    if(!*root)
        return;

    int splitDim = findSplitDim(samples, 0, sampleCount - 1);
    f32 splitValue = findMedian(samples, 0, sampleCount - 1, splitDim);

    (*root)->splitDim = splitDim;
    (*root)->splitValue = splitValue;
    (*root)->parent = NULL;

    size_t leftCount = 0;
    size_t rightCount = 0;
    point** leftSamples = (point**)malloc(sampleCount * sizeof(point*));
    point** rightSamples = (point**)malloc(sampleCount * sizeof(point*));

    for(size_t i = 0; i < sampleCount; ++i)
    {
        if(samples[i]->coords[splitDim] < splitValue)
            leftSamples[leftCount++] = samples[i];
        else
            rightSamples[rightCount++] = samples[i];
    }

    buildSketch(&(*root)->data.internal.left, leftSamples, leftCount, levels - 1);
    buildSketch(&(*root)->data.internal.right, rightSamples, rightCount, levels - 1);

    if((*root)->data.internal.left)
        (*root)->data.internal.left->parent = *root;

    if((*root)->data.internal.right)
        (*root)->data.internal.right->parent = *root;

    free(leftSamples);
    free(rightSamples);
}

static u32 getBucket(KDNode* sketch, point* p)
{
    u32 id = 0;
    KDNode* current = sketch;
    int level = 0;

    while(current && level < SKETCH_HEIGHT && current->type != LEAF)
    {
        id <<= 1;
        if(p->coords[current->splitDim] >= current->splitValue)
        {
            id |= 1;
            current = current->data.internal.right;
        }
        else
        {
            current = current->data.internal.left;
        }

        ++level;
    }

    return id;
}

static KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, int depth)
{
    size_t size = end - start + 1;

    if(size <= LEAF_WRAP_THRESHOLD)
        return createLeafNode(&points[start], size);

    KDNode* node = (KDNode*)malloc(sizeof(KDNode));
    if(!node)
        return NULL;

    int splitDim = findSplitDim(points, start, end);
    f32 splitValue = findMedian(points, start, end, splitDim);

    node->splitDim = splitDim;
    node->splitValue = splitValue;
    node->parent = NULL;

    size_t mid = parallelPartition(points, start, end, splitDim, splitValue);

    #pragma omp parallel sections
    {
        #pragma omp section
        {
            node->data.internal.left = buildTreeParallelPlain(points, start, mid - 1, depth + 1);
            if(node->data.internal.left)
                node->data.internal.left->parent = node;
        }
        #pragma omp section
        {
            node->data.internal.right = buildTreeParallelPlain(points, mid, end, depth + 1);
            if(node->data.internal.right)
                node->data.internal.right->parent = node;
        }
    }

    return node;
}

static f32 findMedian(point** points, size_t start, size_t end, int dim)
{
    size_t size = end - start + 1;
    size_t mid = start + size / 2;

    qsort_r(&points[start], size, sizeof(point*), compareByDim, &dim);

    return points[mid]->coords[dim];
}

static int findSplitDim(point** points, size_t start, size_t end)
{
    f32 minCoords[DIMENSIONS], maxCoords[DIMENSIONS];

    for(size_t i = 0; i < DIMENSIONS; ++i)
    {
        minCoords[i] = INFINITY;
        maxCoords[i] = -INFINITY;
    }

    for(size_t i = start; i <= end; ++i)
    {
        for(size_t j = 0; j < DIMENSIONS; ++j)
        {
            f32 val = points[i]->coords[j];
            if(val < minCoords[j])
                minCoords[j] = val;

            if(val > maxCoords[j])
                maxCoords[j] = val;
        }
    }

    int splitDim = 0;
    f32 maxRange = maxCoords[0] - minCoords[0];

    for(size_t i = 1; i < DIMENSIONS; ++i)
    {
        f32 range = maxCoords[i] - minCoords[i];

        if(range > maxRange)
        {
            maxRange = range;
            splitDim = i;
        }
    }

    return splitDim;
}

static size_t parallelPartition(point** points, size_t start, size_t end, int dim, f32 pivot)
{
    size_t size = end - start + 1;

    point** left = (point**)malloc(size * sizeof(point*));
    point** right = (point**)malloc(size * sizeof(point*));

    size_t leftCount = 0;
    size_t rightCount = 0;

    #pragma omp parallel for reduction(+:leftCount, rightCount)
    for(size_t i = start; i <= end; ++i)
        if(points[i]->coords[dim] < pivot)
            left[leftCount++] = points[i];
        else
            right[rightCount++] = points[i];

    #pragma omp parallel for
    for(size_t i = 0; i < leftCount; ++i)
        points[start + i] = left[i];

    #pragma omp parallel for
    for(size_t i = 0; i < rightCount; ++i)
        points[start + leftCount + i] = right[i];

    free(left);
    free(right);

    return start + leftCount;
}

static KDNode* createLeafNode(point** points, size_t size)
{
    KDNode* leaf = (KDNode*)malloc(sizeof(KDNode));
    if(!leaf)
        return NULL;

    leaf->data.leaf.points = (point**)malloc(size * sizeof(point*));
    memcpy(leaf->data.leaf.points, points, size * sizeof(point*));
    leaf->data.leaf.pointCount = size;

    return leaf;
}

static void freeLeafNode(KDNode* node)
{
    if(!node)
        return;

    if(node->type == LEAF)
    {
        if (node->data.leaf.points)
            free(node->data.leaf.points);

        free(node);
    }
}

static void attachSubtree(KDNode* sketch, int bucketId, KDNode* subtree)
{
    if(!sketch || bucketId < 0 || bucketId >= CHUNK_SIZE)
        return;

    KDNode* current = sketch;
    int level = SKETCH_HEIGHT - 1;

    while(level >= 0)
    {
        int bit = (bucketId >> level) & 1;

        if(level == 0)
        {
            if(bit == 0)
            {
                if(current->data.internal.left)
                    freeLeafNode(current->data.internal.left);

                current->data.internal.left = subtree;
            }
            else
            {
                if(current->data.internal.right)
                    freeLeafNode(current->data.internal.right);

                current->data.internal.right = subtree;
            }

            if(subtree)
                subtree->parent = current;
        }
        else
        {
            if(bit == 0)
                current = current->data.internal.left;
            else
                current = current->data.internal.right;
        }

        level--;
    }
}

static int compareByDim(const void* a, const void* b, void* dim)
{
    int d = *(int*)dim;
    point* pa = *(point**)a;
    point* pb = *(point**)b;

    if(pa->coords[d] < pb->coords[d])
        return -1;

    if(pa->coords[d] > pb->coords[d])
        return 1;

    return 0;
}

static u32** computePrefixSum(u32** matrix, size_t rows, size_t cols)
{
    u32** transposed = (u32**)malloc(cols * sizeof(u32*));
    for(size_t j = 0; j < cols; ++j)
    {
        transposed[j] = (u32*)malloc(rows * sizeof(u32));
        for(size_t i = 0; i < rows; ++i)
            transposed[j][i] = matrix[i][j];
    }

    #pragma omp parallel for
    for(size_t j = 0; j < cols; ++j)
    {
        u32 sum = 0;
        for(size_t i = 0; i < rows; ++i)
        {
            u32 current_val = transposed[j][i];
            transposed[j][i] = sum;
            sum += current_val;
        }
    }

    u32* columnPrefixSums = (u32*)calloc(cols + 1, sizeof(u32));

    u32 total = 0;
    for(size_t j = 0; j < cols; ++j)
    {
        columnPrefixSums[j] = total;
        total += transposed[j][rows - 1] + matrix[rows - 1][j];
    }
    columnPrefixSums[cols] = total;

    #pragma omp parallel for
    for(size_t j = 0; j < cols; ++j)
    {
        u32 colOffset = columnPrefixSums[j];
        for(size_t i = 0; i < rows; ++i)
            transposed[j][i] += colOffset;
    }

    u32** result = (u32**)malloc(rows * sizeof(u32*));
    #pragma omp parallel for
    for(size_t i = 0; i < rows; ++i)
    {
        result[i] = (u32*)malloc(cols * sizeof(u32));
        for(size_t j = 0; j < cols; ++j)
            result[i][j] = transposed[j][i];
    }

    free(columnPrefixSums);
    for(size_t j = 0; j < cols; ++j)
        free(transposed[j]);
    free(transposed);

    return result;
}

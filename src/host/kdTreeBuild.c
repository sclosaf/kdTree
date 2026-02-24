#include <stdlib.h>
#include <math.h>
#include <stdbool.h>
#include <string.h>
#include <omp.h>

#include "kdTree/kdTree.h"
#include "kdTree/kdTreeBuild.h"
#include "utils/constants.h"
#include "utils/types.h"

static void assignNodesToGroups(KDNode* node, KDGroup** groups, int numGroups);
static size_t calculateSubtreeSize(KDNode* node);
static int findGroup(size_t size, KDGroup** groups, u8 numGroups);

static KDNode* buildTreeParallel(point** points, size_t size, u16 depth);
static KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, u16 depth);
static void buildSketch(KDNode** root, point** samples, size_t sampleCount, u16 level);
static Bucket* sievePoints(point** points, size_t size, KDNode* sketch);

static u32 getBucket(KDNode* sketch, point* p);

static f32 findMedian(point** points, size_t start, size_t end, u8 dim);
static u8 findSplitDim(point** points, size_t start, size_t end);
static size_t parallelPartition(point** points, size_t start, size_t end, u8 dim, f32 pivot);

static KDNode* createLeafNode(point** points, size_t size);
static void freeLeafNode(KDNode* node);

static void attachSubtree(KDNode* sketch, u16 bucketId, KDNode* subtree);
static int compareByDim(const void* a, const void* b, void* dim);
static u32** computePrefixSum(u32** matrix, size_t rows, size_t cols);

static void freeKDTree(KDNode* node);
static void freeMatrix(void** matrix, size_t rows);

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

    if(!tree->root)
    {
        free(tree);
        return NULL;
    }

    KDGroup** groups = logStarDecompose(tree);
    if(!groups)
    {
        freeKDTree(tree->root);
        free(tree);
        return NULL;
    }

    // KDTree* finalTree = replicate(tree, groups);
    //
    // for(size_t i = 0; groups[i] != NULL; i++)
    // {
    //     if(groups[i]->rootNodes)
    //         free(groups[i]->rootNodes);
    //     free(groups[i]);
    // }
    //
    // free(groups);
    //
    // freeKDTree(tree->root);
    // free(tree);
    //
    // return finalTree;
}

KDGroup** logStarDecompose(KDTree* tree)
{
    if(!tree || !tree->root)
        return NULL;

    size_t totalSize = tree->totalPoints;
    u8 numGroups = 0;

    size_t current = totalSize;
    while(current > LEAF_WRAP_THRESHOLD)
    {
        ++numGroups;
        current = (size_t)log2(current);
    }
    ++numGroups;

    KDGroup** groups = (KDGroup**)malloc((numGroups) * sizeof(KDGroup*));
    if(!groups)
        return NULL;

    for(size_t i = 0; i < numGroups; ++i)
    {
        groups[i] = (KDGroup*)malloc(sizeof(KDGroup));
        if(!groups[i])
        {
            for(size_t j = 0; j < i; ++j)
                free(groups[j]);
            free(groups);
            return NULL;
        }

        groups[i]->groupId = i;
        groups[i]->rootNodes = NULL;
        groups[i]->count = 0;
        groups[i]->minSize = (i == 0) ? 0 : pow(2, pow(2, i - 1));
        groups[i]->maxSize = pow(2, pow(2, i));
    }

    assignNodesToGroups(tree->root, groups, numGroups);

    return groups;
}

static void assignNodesToGroups(KDNode* node, KDGroup** groups, u8 numGroups)
{
    if(!node)
        return;

    if(node->type == INTERNAL)
    {
        size_t subtreeSize = calculateSubtreeSize(node);

        int groupId = findGroup(subtreeSize, groups, numGroups);

        if(groupId >= 0)
        {
            groups[groupId]->rootNodes = realloc(groups[groupId]->rootNodes, (groups[groupId]->count + 1) * sizeof(KDNode*));
            groups[groupId]->rootNodes[groups[groupId]->count++] = node;
        }

        assignNodesToGroups(node->data.internal.left, groups, numGroups);
        assignNodesToGroups(node->data.internal.right, groups, numGroups);
    }
}

static size_t calculateSubtreeSize(KDNode* node)
{
    if(!node)
        return 0;

    if(node->type == LEAF)
        return node->data.leaf.pointsCount;

    return calculateSubtreeSize(node->data.internal.left) + calculateSubtreeSize(node->data.internal.right);
}

static int findGroup(size_t size, KDGroup** groups, u8 numGroups)
{
    for(int i = 0; i < numGroups; i++)
    {
        if(size > groups[i]->minSize && size <= groups[i]->maxSize)
            return i;
    }
    return -1;
}

static KDNode* buildTreeParallel(point** points, size_t size, u16 depth)
{
    if(size <= LEAF_WRAP_THRESHOLD)
        return createLeafNode(points, size);

    size_t sampleCount = CHUNK_SIZE * OVERSAMPLING_RATE;
    point** samples = (point**)malloc(sampleCount * sizeof(point*));
    if(!samples)
        return NULL;

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; ++i)
    {
        size_t index = rand() % size;
        samples[i] = points[index];
    }

    KDNode* sketch = NULL;
    buildSketch(&sketch, samples, sampleCount, SKETCH_HEIGHT);
    free(samples);

    Bucket* buckets = sievePoints(points, size, sketch);
    if(!buckets)
    {
        freeKDTree(sketch);
        return NULL;
    }

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
    if(!countMatrix)
        return NULL;

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
    if(!offsetMatrix)
    {
        freeMatrix((void**)countMatrix, numChunks);
        return NULL;
    }

    u32* bucketOffsets = (u32*)malloc(CHUNK_SIZE * sizeof(u32));

    if(!bucketOffsets)
    {
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    for(size_t j = 0; j < CHUNK_SIZE; ++j)
        bucketOffsets[j] = offsetMatrix[0][j];

    point** sortedPoints = (point**)malloc(size * sizeof(point*));
    if(!sortedPoints)
    {
        free(bucketOffsets);
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

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

    if(!buckets)
    {
        free(sortedPoints);
        free(bucketOffsets);
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    for(size_t j = 0; j < CHUNK_SIZE; ++j)
    {
        size_t start = bucketOffsets[j];
        size_t end = (j < CHUNK_SIZE - 1) ? bucketOffsets[j + 1] : size;

        buckets[j].bucket = &points[start];
        buckets[j].size = end - start;
    }

    free(sortedPoints);
    free(bucketOffsets);

    freeMatrix((void**)countMatrix, numChunks);
    freeMatrix((void**)offsetMatrix, numChunks);

    return buckets;
}

static void buildSketch(KDNode** root, point** samples, size_t sampleCount, u16 levels)
{
    if(levels == 0 || sampleCount <= LEAF_WRAP_THRESHOLD)
    {
        *root = (KDNode*)malloc(sizeof(KDNode));
        if(!*root)
            return;

        (*root)->type = LEAF;
        (*root)->parent = NULL;
        (*root)->data.leaf.points = NULL;
        (*root)->data.leaf.pointsCount = 0;

        return;
    }

    *root = (KDNode*)malloc(sizeof(KDNode));
    if(!*root)
        return;

    u8 splitDim = findSplitDim(samples, 0, sampleCount - 1);
    f32 splitValue = findMedian(samples, 0, sampleCount - 1, splitDim);

    (*root)->type = INTERNAL;
    (*root)->data.internal.splitDim = splitDim;
    (*root)->data.internal.splitValue = splitValue;
    (*root)->parent = NULL;

    size_t leftCount = 0;
    size_t rightCount = 0;

    point** leftSamples = (point**)malloc(sampleCount * sizeof(point*));
    if(!leftSamples)
    {
        free(*root);
        *root = NULL;
        return;
    }

    point** rightSamples = (point**)malloc(sampleCount * sizeof(point*));
    if(!rightSamples)
    {
        free(leftSamples);
        free(*root);
        *root = NULL;
        return;
    }

    for(size_t i = 0; i < sampleCount; ++i)
    {
        if(samples[i]->coords[splitDim] < splitValue)
            leftSamples[leftCount++] = samples[i];
        else
            rightSamples[rightCount++] = samples[i];
    }

    buildSketch(&(*root)->data.internal.left, leftSamples, leftCount, levels - 1);
    buildSketch(&(*root)->data.internal.right, rightSamples, rightCount, levels - 1);

    if((leftCount > 0 && !(*root)->data.internal.left) || (rightCount > 0 && !(*root)->data.internal.right))
    {
        if((*root)->data.internal.left)
            freeKDTree((*root)->data.internal.left);

        if((*root)->data.internal.right)
            freeKDTree((*root)->data.internal.right);

        free(leftSamples);
        free(rightSamples);
        free(*root);
        *root = NULL;
        return;
    }

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
    u16 level = 0;

    while(current && level < SKETCH_HEIGHT && current->type != LEAF)
    {
        id <<= 1;
        if(p->coords[current->data.internal.splitDim] >= current->data.internal.splitValue)
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

static KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, u16 depth)
{
    size_t size = end - start + 1;

    if(size <= LEAF_WRAP_THRESHOLD)
        return createLeafNode(&points[start], size);

    KDNode* node = (KDNode*)malloc(sizeof(KDNode));
    if(!node)
        return NULL;

    u8 splitDim = findSplitDim(points, start, end);
    f32 splitValue = findMedian(points, start, end, splitDim);

    node->type = INTERNAL;
    node->parent = NULL;
    node->data.internal.splitDim = splitDim;
    node->data.internal.splitValue = splitValue;

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

static f32 findMedian(point** points, size_t start, size_t end, u8 dim)
{
    size_t size = end - start + 1;
    size_t mid = start + size / 2;

    qsort_r(&points[start], size, sizeof(point*), compareByDim, &dim);

    return points[mid]->coords[dim];
}

static u8 findSplitDim(point** points, size_t start, size_t end)
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

    u8 splitDim = 0;
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

static size_t parallelPartition(point** points, size_t start, size_t end, u8 dim, f32 pivot)
{
    size_t size = end - start + 1;

    point** left = (point**)malloc(size * sizeof(point*));
    if(!left)
        return start;

    point** right = (point**)malloc(size * sizeof(point*));
    if(!right)
    {
        free(left);
        return start;
    }

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

    leaf->type = LEAF;
    leaf->parent = NULL;

    leaf->data.leaf.points = (point*)malloc(size * sizeof(point));
    if(!leaf->data.leaf.points)
    {
        free(leaf);
        return NULL;
    }

    for(size_t i = 0; i < size; ++i)
        leaf->data.leaf.points[i] = *points[i];

    leaf->data.leaf.pointsCount = size;

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

static void attachSubtree(KDNode* sketch, u16 bucketId, KDNode* subtree)
{
    if(!sketch || bucketId >= CHUNK_SIZE)
        return;

    KDNode* current = sketch;
    for(u16 level = SKETCH_HEIGHT - 1; level > 0; level--)
    {
        bool bit = (bucketId >> level) & 1;

        if(bit == 0)
            current = current->data.internal.left;
        else
            current = current->data.internal.right;
    }

    bool bit = bucketId & 1;

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

static int compareByDim(const void* a, const void* b, void* dim)
{
    u8 d = *(u8*)dim;
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
    if(!transposed)
        return NULL;

    for(size_t j = 0; j < cols; ++j)
    {
        transposed[j] = (u32*)malloc(rows * sizeof(u32));

        if(!transposed[j])
        {
            freeMatrix((void**)transposed, j);
            return NULL;
        }

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

    if(!columnPrefixSums)
    {
        freeMatrix((void**)transposed, cols);
        return NULL;
    }

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
    if(!result)
    {
        free(columnPrefixSums);
        freeMatrix((void**)transposed, cols);
        return NULL;
    }

    bool error = false;
    #pragma omp parallel for
    for(size_t i = 0; i < rows; ++i)
    {
        result[i] = (u32*)malloc(cols * sizeof(u32));

        if(!result[i])
        {
            #pragma omp critical
            error = true;
            continue;
        }

        for(size_t j = 0; j < cols; ++j)
            result[i][j] = transposed[j][i];
    }

    if(error)
    {
        freeMatrix((void**)result, rows);
        free(columnPrefixSums);
        freeMatrix((void**)transposed, cols);
        return NULL;
    }

    free(columnPrefixSums);
    freeMatrix((void**)transposed, cols);

    return result;
}

static void freeKDTree(KDNode* node)
{
    if(!node)
        return;

    if(node->type == INTERNAL)
    {
        freeKDTree(node->data.internal.left);
        freeKDTree(node->data.internal.right);
    }
    else
    {
        if(node->data.leaf.points)
        {
            free(node->data.leaf.points);
            node->data.leaf.points = NULL;
        }
    }

    free(node);
}

static void freeMatrix(void** matrix, size_t rows)
{
    if(!matrix)
        return;

    for(size_t i = 0; i < rows; ++i)
    {
        if(matrix[i])
            free(matrix[i]);
    }
    free(matrix);
}

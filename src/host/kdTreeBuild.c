#include <stdlib.h>
#include <omp.h>

#include "kdTree/kdTree.h"
#include "kdTree/kdTreeBuild.h"
#include "utils/constants.h"
#include "utils/types.h"

static KDNode* buildTreeParallel(point** points, size_t size, int depth);
static BucketArray* sievePoints(point** points, size_t size, KDNode* sketch);
static void buildSketch(KDNode** root, point** samples, size_t sampleCount, int level);
static u32 get_bucket_id(KDNode* sketch, point* p);
static KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, int depth);
static f32 findMedian(point** points, size_t start, size_t end, int dim);
static int findSplitDim(point** points, size_t start, size_t end);
static size_t parallelPartition(point** points, size_t start, size_t end, int dim, f32 pivot);
static BucketArray* createBucketArray(int numBuckets);
static void freeBucketArray(BucketArray* ba);
static KDNode* createLeafNode(point** points, size_t size);
static inline bool isLeaf(KDNode* node);
static void attachSubtree(KDNode* sketch, int bucket_id, KDNode* subtree);
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
    tree->dimensions = DIMENSIONS;
    tree->totalNodes = 0;

    if(size < (1 << SKETCH_HEIGHT) * OVERSAMPLING_RATE)
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

    size_t sampleCount = (1 << SKETCH_HEIGHT) * OVERSAMPLING_RATE;
    point** samples = (point**)malloc(sampleCount * sizeof(point*));

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; i++)
    {
        int index = rand() % size;
        samples[i] = points[index];
    }

    KDNode* sketch = NULL;
    buildSketch(&sketch, samples, sampleCount, SKETCH_HEIGHT);
    free(samples);

    BucketArray* buckets = sievePoints(points, size, sketch);

    // Recursively build subtrees in parallel
    #pragma omp parallel for
    for(int i = 0; i < (1 << SKETCH_HEIGHT); i++) {
        if(buckets->sizes[i] > 0) {
            KDNode* subtree = buildTreeParallel(buckets->points[i],
                                               buckets->sizes[i],
                                               depth + SKETCH_HEIGHT);
            attachSubtree(sketch, i, subtree);
        }
    }

    freeBucketArray(buckets);
    return sketch;
}

// To continue HERE
static BucketArray* sievePoints(point** points, size_t size, KDNode* sketch)
{
    size_t num_chunks = (size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    #pragma omp parallel for
    for(size_t i = 0; i < num_chunks; i++) {
        count_matrix[i] = (u32*)calloc(1 << SKETCH_HEIGHT, sizeof(u32));

        size_t chunk_start = i * CHUNK_SIZE;
        size_t chunk_end = min(chunk_start + CHUNK_SIZE, size);

        // Count points per bucket (lines 13-15)
        for(size_t j = chunk_start; j < chunk_end; j++) {
            u32 bucket_id = get_bucket_id(sketch, points[j]);
            count_matrix[i][bucket_id]++;
        }
    }

    // Compute column-major prefix sum (line 16)
    u32** offset_matrix = computePrefixSum(count_matrix, num_chunks, 1 << SKETCH_HEIGHT);

    // Get bucket offsets (line 17)
    u32* bucket_offsets = (u32*)malloc((1 << SKETCH_HEIGHT) * sizeof(u32));
    for(int j = 0; j < (1 << SKETCH_HEIGHT); j++) {
        bucket_offsets[j] = offset_matrix[0][j];
    }

    // Allocate destination array
    point** sorted_points = (point**)malloc(size * sizeof(point*));

    // Move points to final positions (lines 18-23)
    #pragma omp parallel for
    for(size_t i = 0; i < num_chunks; i++) {
        size_t chunk_start = i * CHUNK_SIZE;
        size_t chunk_end = min(chunk_start + CHUNK_SIZE, size);

        for(size_t j = chunk_start; j < chunk_end; j++) {
            u32 bucket_id = get_bucket_id(sketch, points[j]);
            u32 dest_idx = offset_matrix[i][bucket_id];
            sorted_points[dest_idx] = points[j];
            offset_matrix[i][bucket_id]++;
        }
    }

    // Copy back to original array (line 24)
    memcpy(points, sorted_points, size * sizeof(point*));

    Bucket* buckets = malloc((1 << SKETCH_HEIGHT) * sizeof(Bucket);
    buckets->bucket = malloc((1 << SKETCH_HEIGHT) * sizeof(point));

    // Create result slices (lines 25-27)
    for(int j = 0; j < (1 << SKETCH_HEIGHT); j++) {
        size_t start = bucket_offsets[j];
        size_t end = (j < (1 << SKETCH_HEIGHT) - 1) ? bucket_offsets[j + 1] : size;

        result->points[j] = &points[start];
        result->sizes[j] = end - start;
    }

    // Cleanup
    free(sorted_points);
    free(bucket_offsets);
    for(size_t i = 0; i < num_chunks; i++) {
        free(count_matrix[i]);
        free(offset_matrix[i]);
    }
    free(count_matrix);
    free(offset_matrix);

    return result;
}

static void buildSketch(KDNode** root, point** samples, size_t sampleCount, int levels)
{
    if(levels == 0 || sampleCount <= LEAF_WRAP_THRESHOLD)
    {
        *root = createLeafNode(samples, sampleCount);
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
    size_t rightCount
    point** leftSamples = (point**)malloc(sampleCount * sizeof(point*));
    point** rightSamples = (point**)malloc(sampleCount * sizeof(point*));

    for(size_t i = 0; i < sampleCount; i++)
    {
        if(samples[i]->coords[splitDim] < splitValue)
            left_samples[leftCount++] = samples[i];
        else
            right_samples[rightCount++] = samples[i];
    }

    buildSketch(&(*root)->data.internal.left, left_samples, left_count, levels - 1);
    buildSketch(&(*root)->data.internal.right, right_samples, sampleCount - left_count, levels - 1);

    free(leftSamples);
    free(rightSamples);
}

static u32 get_bucket_id(KDNode* sketch, point* p)
{
    u32 id = 0;
    KDNode* current = sketch;
    int level = 0;

    while(current && level < SKETCH_HEIGHT && !isLeaf(current)) {
        id <<= 1;
        if(p->coords[current->splitDim] >= current->splitValue) {
            id |= 1;
            current = current->data.internal.right;
        } else {
            current = current->data.internal.left;
        }
        level++;
    }

    return id;
}

static KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, int depth)
{
    size_t size = end - start + 1;

    if(size <= LEAF_WRAP_THRESHOLD)
        return createLeafNode(&points[start], size);

    KDNode* node = (KDNode*)malloc(sizeof(KDNode));
    if(!node) return NULL;

    // Find split dimension and median
    int split_dim = depth % DIMENSIONS;  // Simple cycling through dimensions
    f32 split_value = findMedian(points, start, end, split_dim);

    node->splitDim = split_dim;
    node->splitValue = split_value;
    node->parent = NULL;

    // Partition points (parallel partition)
    size_t mid = parallelPartition(points, start, end, split_dim, split_value);

    // Recursively build subtrees in parallel
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

    for(int i = 0; i < DIMENSIONS; i++)
    {
        minCoords[i] = INFINITY;
        maxCoords[i] = -INFINITY;
    }

    for(size_t i = start; i <= end; i++)
    {
        for(int j = 0; j < DIMENSIONS; j++)
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

    for(int i = 1; i < DIMENSIONS; i++)
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

// Helper function for parallel partition
static size_t parallelPartition(point** points, size_t start, size_t end, int dim, f32 pivot)
{
    // Simple sequential partition for now
    // In practice, implement parallel partition
    size_t i = start;
    size_t j = end;

    while(i <= j) {
        while(i <= end && points[i]->coords[dim] < pivot) i++;
        while(j >= start && points[j]->coords[dim] >= pivot) j--;

        if(i < j) {
            point* temp = points[i];
            points[i] = points[j];
            points[j] = temp;
        }
    }

    return i;
}

// Helper functions for bucket array management
static BucketArray* createBucketArray(int numBuckets)
{


static void freeBucketArray(BucketArray* ba)
{
    free(ba->points);
    free(ba->sizes);
    free(ba);
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

static inline bool isLeaf(KDNode* node)
{
    return node->type == LEAF;
}

static void attachSubtree(KDNode* sketch, int bucket_id, KDNode* subtree)
{
    // Find the external node in sketch corresponding to bucket_id
    // and replace it with subtree
    // This is a simplified version - actual implementation would need
    // to traverse the sketch to find the correct position
    KDNode* current = sketch;
    int level = SKETCH_HEIGHT - 1;

    while(level >= 0) {
        int bit = (bucket_id >> level) & 1;
        if(bit == 0) {
            if(level == 0) {
                current->data.internal.left = subtree;
                if(subtree) subtree->parent = current;
            } else {
                current = current->data.internal.left;
            }
        } else {
            if(level == 0) {
                current->data.internal.right = subtree;
                if(subtree) subtree->parent = current;
            } else {
                current = current->data.internal.right;
            }
        }
        level--;
    }
}

// Comparison function for qsort_r
static int compareByDim(const void* a, const void* b, void* dim)
{
    int d = *(int*)dim;
    point* pa = *(point**)a;
    point* pb = *(point**)b;

    if(pa->coords[d] < pb->coords[d]) return -1;
    if(pa->coords[d] > pb->coords[d]) return 1;
    return 0;
}

// Helper for prefix sum computation
static u32** computePrefixSum(u32** matrix, size_t rows, size_t cols)
{
    u32** result = (u32**)malloc(rows * sizeof(u32*));

    // First, compute row-wise prefix sums
    #pragma omp parallel for
    for(size_t i = 0; i < rows; i++) {
        result[i] = (u32*)malloc(cols * sizeof(u32));
        u32 sum = 0;
        for(size_t j = 0; j < cols; j++) {
            result[i][j] = sum;
            sum += matrix[i][j];
        }
    }

    // Then, compute column-wise prefix sums (transpose approach)
    u32** transposed = (u32**)malloc(cols * sizeof(u32*));
    for(size_t j = 0; j < cols; j++) {
        transposed[j] = (u32*)malloc(rows * sizeof(u32));
        for(size_t i = 0; i < rows; i++) {
            transposed[j][i] = result[i][j];
        }
    }

    // Compute prefix sums on transposed
    #pragma omp parallel for
    for(size_t j = 0; j < cols; j++) {
        u32 sum = 0;
        for(size_t i = 0; i < rows; i++) {
            u32 temp = transposed[j][i];
            transposed[j][i] = sum;
            sum += temp;
        }
    }

    // Transpose back
    #pragma omp parallel for
    for(size_t i = 0; i < rows; i++) {
        for(size_t j = 0; j < cols; j++) {
            result[i][j] = transposed[j][i];
        }
    }

    // Cleanup
    for(size_t j = 0; j < cols; j++) {
        free(transposed[j]);
    }
    free(transposed);

    return result;
}

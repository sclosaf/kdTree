#include <stdlib.h>
#include <stdbool.h>
#include <omp.h>
#include <math.h>

#include "kdTree/utils.h"

#include "environment/init.h"

float findMedian(point** points, size_t start, size_t end, uint8_t dim)
{
    size_t size = end - start + 1;
    size_t mid = start + size / 2;

    qsort_r(&points[start], size, sizeof(point*), compareByDim, &dim);

    return points[mid]->coords[dim];
}

uint8_t findSplitDim(point** points, size_t start, size_t end)
{
    uint32_t dims = getConfig()->dimensions;

    float* minCoords = malloc(dims * sizeof(float));
    float* maxCoords = malloc(dims * sizeof(float));

    for(size_t i = 0; i < dims; ++i)
    {
        minCoords[i] = INFINITY;
        maxCoords[i] = -INFINITY;
    }

    for(size_t i = start; i <= end; ++i)
    {
        for(size_t j = 0; j < dims; ++j)
        {
            float val = points[i]->coords[j];
            if(val < minCoords[j])
                minCoords[j] = val;

            if(val > maxCoords[j])
                maxCoords[j] = val;
        }
    }

    uint8_t splitDim = 0;
    float maxRange = maxCoords[0] - minCoords[0];

    for(size_t i = 1; i < dims; ++i)
    {
        float range = maxCoords[i] - minCoords[i];

        if(range > maxRange)
        {
            maxRange = range;
            splitDim = i;
        }
    }

    return splitDim;
}

int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups)
{
    for(int i = 0; i < numGroups; ++i)
        if(size > groups[i]->minSize && size <= groups[i]->maxSize)
            return i;

    return -1;
}

uint32_t getBucket(KDNode* sketch, point* p)
{
    uint32_t id = 0;
    KDNode* current = sketch;
    uint16_t level = 0;

    while(current && level < getConfig()->sketchHeight && current->type != LEAF)
    {
        id <<= 1;
        if(p->coords[current->data.internal.splitDim] >= current->data.internal.splitValue)
        {
            id |= 1;
            current = current->data.internal.right;
        }
        else
            current = current->data.internal.left;

        ++level;
    }

    return id;
}

size_t parallelPartition(point** points, size_t start, size_t end, uint8_t dim, float pivot)
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

int compareByDim(const void* a, const void* b, void* dim)
{
    uint8_t d = *(uint8_t*)dim;
    point* pa = *(point**)a;
    point* pb = *(point**)b;

    if(pa->coords[d] < pb->coords[d])
        return -1;

    if(pa->coords[d] > pb->coords[d])
        return 1;

    return 0;
}

size_t calculateSubtreeSize(KDNode* node)
{
    if(!node)
        return 0;

    if(node->type == LEAF)
        return node->data.leaf.pointsCount;

    return calculateSubtreeSize(node->data.internal.left) + calculateSubtreeSize(node->data.internal.right);
}

uint32_t** computePrefixSum(uint32_t** matrix, size_t rows, size_t cols)
{
    uint32_t** transposed = (uint32_t**)malloc(cols * sizeof(uint32_t*));
    if(!transposed)
        return NULL;

    for(size_t j = 0; j < cols; ++j)
    {
        transposed[j] = (uint32_t*)malloc(rows * sizeof(uint32_t));

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
        uint32_t sum = 0;
        for(size_t i = 0; i < rows; ++i)
        {
            uint32_t current_val = transposed[j][i];
            transposed[j][i] = sum;
            sum += current_val;
        }
    }

    uint32_t* columnPrefixSums = (uint32_t*)calloc(cols + 1, sizeof(uint32_t));

    if(!columnPrefixSums)
    {
        freeMatrix((void**)transposed, cols);
        return NULL;
    }

    uint32_t total = 0;
    for(size_t j = 0; j < cols; ++j)
    {
        columnPrefixSums[j] = total;
        total += transposed[j][rows - 1] + matrix[rows - 1][j];
    }

    columnPrefixSums[cols] = total;

    #pragma omp parallel for
    for(size_t j = 0; j < cols; ++j)
    {
        uint32_t colOffset = columnPrefixSums[j];
        for(size_t i = 0; i < rows; ++i)
            transposed[j][i] += colOffset;
    }

    uint32_t** result = (uint32_t**)malloc(rows * sizeof(uint32_t*));
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
        result[i] = (uint32_t*)malloc(cols * sizeof(uint32_t));

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

uint32_t getNodeSize(KDNode* node)
{
    if(!node)
        return 0;

    if(node->type == LEAF)
        return node->data.leaf.pointsCount;

    return node->data.internal.approximateCounter;
}

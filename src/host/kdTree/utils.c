#include <stdlib.h>
#include <stdbool.h>
#include <omp.h>
#include <math.h>

#include "host/kdTree/utils.h"
#include "host/kdTree/free.h"

#include "host/environment/init.h"

#include "host/management/logging.h"

float findMedian(point** points, size_t start, size_t end, uint8_t dim)
{
    logMessage("Finding median", DEBUG);

    if(!points || start > end)
    {
        logMessage("Invalid input to findMedian", ERROR);
        return 0.0f;
    }

    size_t size = end - start + 1;
    size_t mid = start + size / 2;

    for(size_t i = start; i <= end; i++)
    {
        if(!points[i])
        {
            logMessage("Null point found in array", ERROR);
            return 0.0f;
        }
    }

    qsort_r(&points[start], size, sizeof(point*), compareByDim, &dim);

    if(mid > end || !points[mid])
    {
        logMessage("Invalid median index", ERROR);
        return 0.0f;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Median found: %.4f at index %zu on dim %u", points[mid]->coords[dim], mid, dim);
    logMessage(msg, INFO);

    return points[mid]->coords[dim];
}

uint8_t findSplitDim(point** points, size_t start, size_t end)
{
    logMessage("Finding split dimension", DEBUG);

    if(!points || start > end)
    {
        logMessage("Invalid input to findSplitDim", ERROR);
        return 0;
    }

    uint32_t dims = getDimensions();
    if(dims == 0)
    {
        logMessage("Invalid dimensions (0)", ERROR);
        return 0;
    }

    float* minCoords = calloc(dims, sizeof(float));
    if(!minCoords)
    {
        logMessage("Failed to allocate minCoords", ERROR);
        return 0;
    }

    float* maxCoords = calloc(dims, sizeof(float));
    if(!maxCoords)
    {
        logMessage("Failed to allocate maxCoords", ERROR);
        free(minCoords);
        return 0;
    }

    logMessage("Allocated min/max coordinate arrays", DEBUG);

    for(size_t i = 0; i < dims; ++i)
    {
        minCoords[i] = INFINITY;
        maxCoords[i] = -INFINITY;
    }

    for(size_t i = start; i <= end; ++i)
    {
        if(!points[i])
        {
            logMessage("Null point encountered", ERROR);
            free(minCoords);
            free(maxCoords);
            return 0;
        }

        for(size_t j = 0; j < dims; ++j)
        {
            float val = points[i]->coords[j];

            if(val < minCoords[j])
                minCoords[j] = val;

            if(val > maxCoords[j])
                maxCoords[j] = val;
        }
    }

    logMessage("Coordinate ranges computed", DEBUG);

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

    free(minCoords);
    free(maxCoords);

    char msg[256];
    snprintf(msg, sizeof(msg), "Split dimension: %u with range %.4f", splitDim, maxRange);
    logMessage(msg, INFO);

    return splitDim;
}

int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups)
{
    logMessage("Finding group for node size", DEBUG);

    if(!groups || numGroups == 0)
    {
        logMessage("Invalid groups array or numGroups is zero", ERROR);
        return -1;
    }

    for(uint8_t i = 0; i < numGroups; ++i)
    {
        if(!groups[i])
            continue;

        if(i == 0 && size >= groups[i]->minSize && size <= groups[i]->maxSize)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Node size %zu assigned to group %u", size, i);
            logMessage(msg, INFO);
            return i;
        }
        else if(i > 0 && size >= groups[i]->minSize && size < groups[i]->maxSize)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Node size %zu assigned to group %u", size, i);
            logMessage(msg, INFO);
            return i;
        }
    }

    if(numGroups > 0 && groups[numGroups - 1] && size < groups[numGroups - 1]->minSize)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Node size %zu falls below all groups, defaulting to group %u", size, numGroups - 1);
        logMessage(msg, INFO);
        return numGroups - 1;
    }

    logMessage("No matching group found for node size", ERROR);
    return -1;
}

uint32_t getBucket(KDNode* sketch, point* p)
{
    logMessage("Computing bucket for point", INFO);

    if(!sketch || !p)
    {
        logMessage("Null sketch or point provided", ERROR);
        return 0;
    }

    uint32_t id = 0;
    KDNode* current = sketch;
    uint16_t level = 0;

    while(current && level < getSketchHeight() && current->type != LEAF)
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

    char msg[256];
    snprintf(msg, sizeof(msg), "Bucket id: %u at depth %u", id, level);
    logMessage(msg, INFO);

    return id;
}

uint32_t getNodeSize(KDNode* node)
{
    logMessage("Retrieving node size", DEBUG);

    if(!node)
    {
        logMessage("Null node provided", ERROR);
        return 0;
    }

    uint32_t size = (node->type == LEAF) ? node->data.leaf.pointsCount : node->data.internal.approximateCounter;

    char msg[256];
    snprintf(msg, sizeof(msg), "Node size: %u", size);
    logMessage(msg, INFO);

    return size;
}

size_t partitionPoints(point** points, size_t start, size_t end, uint8_t dim, float pivot)
{
    logMessage("Partitioning points", DEBUG);

    size_t size = end - start + 1;
    point** left = (point**)malloc(size * sizeof(point*));
    if(!left)
    {
        logMessage("Failed to allocate left partition buffer", ERROR);
        return start;
    }

    point** right = (point**)malloc(size * sizeof(point*));
    if(!right)
    {
        logMessage("Failed to allocate right partition buffer", ERROR);
        free(left);
        return start;
    }

    int numThreads = omp_get_max_threads();
    size_t* leftOffsets = (size_t*)calloc(numThreads + 1, sizeof(size_t));
    size_t* rightOffsets = (size_t*)calloc(numThreads + 1, sizeof(size_t));

    if(!leftOffsets || !rightOffsets)
    {
        logMessage("Failed to allocate offset arrays", ERROR);
        free(left); free(right);
        free(leftOffsets); free(rightOffsets);
        return start;
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        size_t localLeft = 0;
        size_t localRight = 0;

        #pragma omp for
        for(size_t i = start; i <= end; ++i)
        {
            if(points[i] && points[i]->coords[dim] < pivot)
                ++localLeft;
            else
                ++localRight;
        }

        leftOffsets[tid] = localLeft;
        rightOffsets[tid] = localRight;
    }

    size_t leftTotal = 0;
    size_t rightTotal = 0;
    for(int t = 0; t < numThreads; ++t)
    {
        size_t l = leftOffsets[t];
        size_t r = rightOffsets[t];

        leftOffsets[t] = leftTotal;
        rightOffsets[t] = rightTotal;

        leftTotal += l;
        rightTotal += r;
    }

    leftOffsets[numThreads] = leftTotal;
    rightOffsets[numThreads] = rightTotal;

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        size_t leftPos = leftOffsets[tid];
        size_t rightPos = rightOffsets[tid];

        #pragma omp for
        for(size_t i = start; i <= end; ++i)
        {
            if(!points[i])
                continue;

            if(points[i]->coords[dim] < pivot)
                left[leftPos++] = points[i];
            else
                right[rightPos++] = points[i];
        }
    }

    #pragma omp parallel for
    for(size_t i = 0; i < leftTotal; ++i)
        points[start + i] = left[i];

    #pragma omp parallel for
    for(size_t i = 0; i < rightTotal; ++i)
        points[start + leftTotal + i] = right[i];

    free(left);
    free(right);
    free(leftOffsets);
    free(rightOffsets);

    char msg[256];
    snprintf(msg, sizeof(msg), "Partition complete: %zu left, %zu right on dim %u pivot %.4f", leftTotal, rightTotal, dim, pivot);
    logMessage(msg, INFO);

    return start + leftTotal;
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

uint32_t** computePrefixSum(uint32_t** matrix, size_t rows, size_t cols)
{
    logMessage("Computing prefix sum matrix", DEBUG);

    uint32_t** transposed = (uint32_t**)malloc(cols * sizeof(uint32_t*));
    if(!transposed)
    {
        logMessage("Failed to allocate transposed matrix", ERROR);
        return NULL;
    }

    for(size_t j = 0; j < cols; ++j)
    {
        transposed[j] = (uint32_t*)malloc(rows * sizeof(uint32_t));
        if(!transposed[j])
        {
            logMessage("Failed to allocate transposed matrix row", ERROR);
            freeMatrix((void**)transposed, j);
            return NULL;
        }
        for(size_t i = 0; i < rows; ++i)
            transposed[j][i] = matrix[i][j];
    }

    logMessage("Matrix transposed successfully", DEBUG);

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

    logMessage("Column prefix sums computed", DEBUG);

    uint32_t* columnPrefixSums = (uint32_t*)calloc(cols + 1, sizeof(uint32_t));
    if(!columnPrefixSums)
    {
        logMessage("Failed to allocate column prefix sums array", ERROR);
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

    char msg[256];
    snprintf(msg, sizeof(msg), "Total prefix sum across all columns: %u", total);
    logMessage(msg, INFO);

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
        logMessage("Failed to allocate result matrix", ERROR);
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
        logMessage("Failed to allocate one or more result matrix rows", ERROR);
        freeMatrix((void**)result, rows);
        free(columnPrefixSums);
        freeMatrix((void**)transposed, cols);
        return NULL;
    }

    free(columnPrefixSums);
    freeMatrix((void**)transposed, cols);

    snprintf(msg, sizeof(msg), "Prefix sum matrix computed successfully: %zu rows x %zu cols", rows, cols);
    logMessage(msg, INFO);

    return result;
}

void collectAllPoints(KDNode* node, point*** collector, size_t* count, size_t* capacity)
{
    logMessage("Collecting all points from subtree", DEBUG);

    if(!node)
        return;

    if(node->type == LEAF)
    {
        for(size_t i = 0; i < node->data.leaf.pointsCount; ++i)
        {
            if(*count >= *capacity)
            {
                *capacity = (*capacity == 0) ? 1024 : *capacity * 2;
                *collector = (point**)realloc(*collector, *capacity * sizeof(point*));
                if(!*collector)
                {
                    logMessage("Failed to reallocate points collector", ERROR);
                    return;
                }

                char msg[256];
                snprintf(msg, sizeof(msg), "Points collector reallocated to capacity %zu", *capacity);
                logMessage(msg, DEBUG);
            }

            (*collector)[(*count)++] = &node->data.leaf.points[i];
        }

        char msg[256];
        snprintf(msg, sizeof(msg), "Collected %zu points from leaf, total so far: %zu", node->data.leaf.pointsCount, *count);
        logMessage(msg, INFO);
    }
    else
    {
        collectAllPoints(node->data.internal.left, collector, count, capacity);
        collectAllPoints(node->data.internal.right, collector, count, capacity);
    }
}

void collectAllLeaves(KDNode* node, KDNode*** collector, size_t* count, size_t* capacity)
{
    logMessage("Collecting all leaves from subtree", DEBUG);

    if(!node)
        return;

    if(node->type == LEAF)
    {
        if(*count >= *capacity)
        {
            *capacity = (*capacity == 0) ? 1024 : *capacity * 2;
            *collector = (KDNode**)realloc(*collector, *capacity * sizeof(KDNode*));
            if(!*collector)
            {
                logMessage("Failed to reallocate leaves collector", ERROR);
                return;
            }

            char msg[256];
            snprintf(msg, sizeof(msg), "Leaves collector reallocated to capacity %zu", *capacity);
            logMessage(msg, DEBUG);
        }

        (*collector)[(*count)++] = node;

        char msg[256];
        snprintf(msg, sizeof(msg), "Leaf collected, total leaves so far: %zu", *count);
        logMessage(msg, INFO);
    }
    else
    {
        collectAllLeaves(node->data.internal.left, collector, count, capacity);
        collectAllLeaves(node->data.internal.right, collector, count, capacity);
    }
}

int findGroupForSize(size_t size, KDGroup** groups)
{
    logMessage("Finding group for size", DEBUG);

    if(!groups)
    {
        logMessage("Null groups array provided", ERROR);
        return -1;
    }

    for(int i = 0; groups[i] != NULL; ++i)
    {
        if(groups[i]->minSize <= size && size < groups[i]->maxSize)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Size %zu matched group %d", size, i);
            logMessage(msg, INFO);
            return i;
        }
    }

    logMessage("No group matched for size", ERROR);
    return -1;
}

bool isNodeInSubtree(KDNode* node, KDNode* potentialRoot)
{
    logMessage("Checking if node is in subtree", DEBUG);

    if(!node || !potentialRoot)
    {
        logMessage("Null node or potentialRoot provided", ERROR);
        return false;
    }

    KDNode* current = node;
    while(current)
    {
        if(current == potentialRoot)
        {
            logMessage("Node found in subtree", INFO);
            return true;
        }

        current = current->parent;
    }

    logMessage("Node not found in subtree", INFO);
    return false;
}

KDGroup* findGroupForNode(KDNode* node)
{
    logMessage("Finding group for node", DEBUG);

    Data* data = getData();
    if(!data->tree || !data->tree->groups)
    {
        logMessage("Tree or groups not initialized", ERROR);
        return NULL;
    }

    size_t nodeSize = getNodeSize(node);
    int groupId = findGroupForSize(nodeSize, data->tree->groups);

    if(groupId < 0)
    {
        logMessage("No group found for node", ERROR);
        return NULL;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Node of size %zu assigned to group %d", nodeSize, groupId);
    logMessage(msg, INFO);

    return data->tree->groups[groupId];
}

uint16_t calculateGroupHeight(uint8_t groupId)
{
    logMessage("Calculating group height", DEBUG);

    double value = getNPim();
    for(uint8_t i = 0; i < groupId; ++i)
    {
        value = log2(value);
        if(value <= 1.0)
        {
            value = 1.0;
            break;
        }
    }

    uint16_t height = (uint16_t)(value + 1);

    char msg[256];
    snprintf(msg, sizeof(msg), "Group %u height: %u", groupId, height);
    logMessage(msg, INFO);

    return height;
}

uint8_t calculateNumGroups()
{
    logMessage("Calculating number of groups", DEBUG);

    uint8_t groups = 0;
    double value = getNPim();
    while(value > 1.0)
    {
        value = log2(value);
        ++groups;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Number of groups: %u", groups);
    logMessage(msg, INFO);

    return groups;
}

KDNode* findLeafNodeByIndex(KDNode* sketch, uint32_t index)
{
    if(!sketch) return NULL;

    KDNode* current = sketch;
    uint16_t height = getSketchHeight();
    uint16_t level = 0;

    if(height == 0)
        return sketch;

    while(current && level < height)
    {
        if(current->type == LEAF)
            return current;

        if(current->type != INTERNAL)
            return NULL;

        uint32_t bit = (index >> (height - level - 1)) & 1;

        if(bit == 0)
            current = current->data.internal.left;
        else
            current = current->data.internal.right;

        ++level;
    }

    return current;
}

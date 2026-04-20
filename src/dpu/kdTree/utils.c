#include <stdlib.h>
#include <string.h>
#include <alloc.h>
#include <float.h>
#include <defs.h>
#include <stdio.h>

#include "dpu/kdTree/utils.h"
#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"

static uint32_t seed = 0;

static size_t partitionFloat(float* arr, size_t left, size_t right)
{
    float pivot = arr[right];
    size_t i = left;

    for(size_t j = left; j < right; ++j)
    {
        if(arr[j] <= pivot)
        {
            float temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
            ++i;
        }
    }

    float temp = arr[i];
    arr[i] = arr[right];
    arr[right] = temp;

    return i;
}

float findMedian(point** points, size_t start, size_t end, uint8_t dim)
{
    size_t size = end - start + 1;

    if (size == 0)
    {
        DPU_LOG("findMedian: Empty range, returning 0.0");
        return 0.0f;
    }

    size_t mid = start + size / 2;
    size_t k = mid - start;

    float* values = (float*)mem_alloc(size * sizeof(float));
    if(!values)
    {
        DPU_LOG("findMedian: ERR - mem_alloc failed for values array");
        return 0.0f;
    }

    for(size_t i = 0; i < size; ++i)
        values[i] = points[start + i]->coords[dim];

    size_t left = 0;
    size_t right = size - 1;

    while(left < right)
    {
        size_t pivotIndex = partitionFloat(values, left, right);

        if(k == pivotIndex)
            break;
        else if(k < pivotIndex)
            right = (pivotIndex == 0) ? 0 : pivotIndex - 1;
        else
            left = pivotIndex + 1;
    }

    float median = values[k];

    return median;
}

uint8_t findSplitDim(point** points, size_t start, size_t end)
{
    uint32_t dims = getDimensions();

    float* minCoords = mem_alloc(dims * sizeof(float));
    float* maxCoords = mem_alloc(dims * sizeof(float));

    if(!minCoords || !maxCoords)
    {
        DPU_LOG("findSplitDim: ERR - mem_alloc failed for min/max coords");
        return 0;
    }

    for(size_t i = 0; i < dims; ++i)
    {
        minCoords[i] = FLT_MAX;
        maxCoords[i] = -FLT_MAX;
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

size_t partitionPoints(point** points, size_t start, size_t end, uint8_t dim, float pivot)
{
    size_t size = end - start + 1;
    size_t leftCount = 0;

    point** temp = (point**)mem_alloc(size * sizeof(point*));
    if(!temp)
    {
        DPU_LOG("partitionPoints: ERR - mem_alloc failed for temp array");
        return start;
    }

    for(size_t i = start; i <= end; ++i)
        if(points[i]->coords[dim] < pivot)
            ++leftCount;

    size_t leftIdx = 0;
    size_t rightIdx = leftCount;

    for(size_t i = start; i <= end; ++i)
    {
        if(points[i]->coords[dim] < pivot)
            temp[leftIdx++] = points[i];
        else
            temp[rightIdx++] = points[i];
    }

    for(size_t i = 0; i < size; ++i)
        points[start + i] = temp[i];

    return start + leftCount;
}

uint32_t getBucket(KDNode* sketch, float* point)
{
    uint32_t id = 0;
    KDNode* current = sketch;
    uint16_t level = 0;
    uint16_t sketchHeight = getSketchHeight();

    while(current && level < sketchHeight && current->type != LEAF)
    {
        id <<= 1;

        if(point[current->data.internal.splitDim] >= current->data.internal.splitValue)
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

int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups)
{
    if(!groups || numGroups == 0)
    {
        DPU_LOG("findGroup: ERR - Invalid groups or numGroups");
        return -1;
    }

    for(uint8_t i = 0; i < numGroups; ++i)
    {
        if(!groups[i])
            continue;

        if(i == 0 && size >= groups[i]->minSize && size <= groups[i]->maxSize)
            return i;
        else if(i > 0 && size >= groups[i]->minSize && size < groups[i]->maxSize)
            return i;
    }

    if(numGroups > 0 && groups[numGroups - 1] && size < groups[numGroups - 1]->minSize)
    {
        DPU_LOG("findGroup: Falling back to last group %d", numGroups - 1);
        return numGroups - 1;
    }

    DPU_LOG("findGroup: No group found, returning -1");
    return -1;
}

uint32_t getNodeSize(KDNode* node)
{
    if(!node)
    {
        DPU_LOG("getNodeSize: NULL node, returning 0");
        return 0;
    }

    uint32_t size;
    if(node->type == LEAF)
        size = node->data.leaf.pointsCount;
    else
        size = node->data.internal.approximateCounter;

    return size;
}

uint16_t calculateGroupHeight(uint8_t groupId)
{
    uint8_t value = getNPim();

    for(uint8_t i = 0; i < groupId; ++i)
    {
        value = log2f(value);
        if(value <= 1.0)
        {
            value = 1.0;
            break;
        }
    }

    uint16_t result = (uint16_t)(value + 1);
    return result;
}

static uint32_t countNodes(KDNode* node)
{
    if(!node)
        return 0;

    if(node->type == LEAF)
    {
        DPU_LOG("countNodes: Leaf node, returning 1");
        return 1;
    }

    uint32_t count = 1 + countNodes(node->data.internal.left) + countNodes(node->data.internal.right);
    return count;
}

static void fillAddressMap(KDNode* node, uint64_t* map, uint32_t* index)
{
    if(!node)
        return;

    map[(*index)++] = (uint64_t)(uintptr_t)node;

    if(node->type == INTERNAL)
    {
        fillAddressMap(node->data.internal.left, map, index);
        fillAddressMap(node->data.internal.right, map, index);
    }
}

uint64_t* buildNodeAddressMap(KDNode* root, size_t* mapSize)
{
    if(!root)
    {
        DPU_LOG("buildNodeAddressMap: root is NULL");
        *mapSize = 0;
        return NULL;
    }

    uint32_t totalNodes = countNodes(root);
    *mapSize = totalNodes;

    uint64_t* map = (uint64_t*)mem_alloc(totalNodes * sizeof(uint64_t));
    if(!map)
    {
        DPU_LOG("buildNodeAddressMap: ERR - mem_alloc failed for map (%u bytes)", totalNodes * sizeof(uint64_t));
        return NULL;
    }

    uint32_t currentIndex = 0;
    fillAddressMap(root, map, &currentIndex);

    return map;
}

float log2f(float x)
{
    if (x <= 0.0)
        return 0.0/0.0;

    if(x == 1.0)
        return 0.0;

    float ln2 = 0.693147180559945309417;
    size_t iter = 20;

    int exponent = 0;
    float mantissa = x;

    if(mantissa >= 1.0)
    {
        while (mantissa >= 1.0)
        {
            mantissa *= 0.5;
            ++exponent;
        }
    }
    else
    {
        while(mantissa < 0.5)
        {
            mantissa *= 2.0;
            --exponent;
        }
    }

    mantissa -= 0.5;
    float z = mantissa / 0.5;
    float sum = 0.0;
    float term = z;
    float power = z;
    int sign = 1;

    for(int k = 1; k <= iter; ++k)
    {
        if(k == 1)
            sum += term;
        else
        {
            power *= z;
            sign = -sign;
            term = (sign * power) / k;
            sum += term;
        }
    }

    float result = ((-ln2 + sum) / ln2) + exponent;
    return result;
}

float fabsf(float x)
{
    float result = (x < 0) ? -x : x;
    return result;
}

void setupRand()
{
    if(!seed)
        seed = me() + 1;
}

uint32_t rand()
{
    uint32_t result = seed = (1103515245 * seed + 12345) & 0x7fffffff;
    return result;
}

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>

#include "host/query/range.h"
#include "host/kdTree/search.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/build.h"
#include "host/kdTree/free.h"
#include "host/kdTree/distribute.h"
#include "host/environment/init.h"
#include "host/management/logging.h"

typedef struct RangeStackEntry
{
    KDNode* node;
    float* nodeMin;
    float* nodeMax;
    struct RangeStackEntry* next;
} RangeStackEntry;

static void pushEntry(RangeStackEntry** stack, KDNode* node, float* nodeMin, float* nodeMax)
{
    logMessage("Pushing entry onto range stack", DEBUG);

    RangeStackEntry* entry = (RangeStackEntry*)malloc(sizeof(RangeStackEntry));
    if(!entry)
    {
        logMessage("Failed to allocate stack entry", ERROR);
        return;
    }

    uint8_t dims = getDimensions();
    entry->node = node;
    entry->nodeMin = (float*)malloc(dims * sizeof(float));
    entry->nodeMax = (float*)malloc(dims * sizeof(float));

    memcpy(entry->nodeMin, nodeMin, dims * sizeof(float));
    memcpy(entry->nodeMax, nodeMax, dims * sizeof(float));

    entry->next = *stack;
    *stack = entry;
}

static RangeStackEntry* popEntry(RangeStackEntry** stack)
{
    if(!*stack)
        return NULL;

    RangeStackEntry* entry = *stack;
    *stack = entry->next;

    logMessage("Popped entry from range stack", DEBUG);
    return entry;
}

static void freeEntry(RangeStackEntry* entry)
{
    if(entry)
    {
        free(entry->nodeMin);
        free(entry->nodeMax);
        free(entry);
        logMessage("Freed stack entry", DEBUG);
    }
}

static void initializeRootBoundingBox(float* nodeMin, float* nodeMax)
{
    uint8_t dims = getDimensions();
    float minCoord = getMinCoord();
    float maxCoord = getMaxCoord();

    for(uint8_t d = 0; d < dims; ++d)
    {
        nodeMin[d] = minCoord;
        nodeMax[d] = maxCoord;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Root bounding box initialized: min=%f, max=%f", minCoord, maxCoord);
    logMessage(msg, INFO);
}

static void expandBoundingBox(KDNode* child, float* childMin, float* childMax, float* parentMin, float* parentMax, uint8_t splitDim, float splitValue, bool isLeftChild)
{
    uint8_t dims = getDimensions();

    for(uint8_t d = 0; d < dims; ++d)
    {
        childMin[d] = parentMin[d];
        childMax[d] = parentMax[d];
    }

    if(isLeftChild)
        childMax[splitDim] = splitValue;
    else
        childMin[splitDim] = splitValue;

    char msg[128];
    snprintf(msg, sizeof(msg), "Bounding box expanded: splitDim=%d, splitValue=%f, isLeft=%d", splitDim, splitValue, isLeftChild);
    logMessage(msg, INFO);
}

static void computeLeafBoundingBox(KDNode* leaf, float* leafMin, float* leafMax)
{
    logMessage("Computing leaf bounding box", DEBUG);

    if(!leaf || leaf->type != LEAF)
    {
        logMessage("Invalid leaf node for bounding box computation", ERROR);
        return;
    }

    uint8_t dims = getDimensions();

    initializeRootBoundingBox(leafMin, leafMax);

    KDNode* path[256];
    int pathLen = 0;
    KDNode* current = leaf;

    while(current && pathLen < 256)
    {
        path[pathLen++] = current;
        current = current->parent;
    }

    float nodeMin[dims], nodeMax[dims];
    initializeRootBoundingBox(nodeMin, nodeMax);

    for(int i = pathLen - 1; i > 0; --i)
    {
        KDNode* parent = path[i];
        KDNode* child = path[i - 1];

        if(parent->type == INTERNAL)
        {
            bool isLeft = (parent->data.internal.left == child);
            float newMin[dims], newMax[dims];

            expandBoundingBox(child, newMin, newMax, nodeMin, nodeMax, parent->data.internal.splitDim, parent->data.internal.splitValue, isLeft);

            memcpy(nodeMin, newMin, dims * sizeof(float));
            memcpy(nodeMax, newMax, dims * sizeof(float));
        }
    }

    memcpy(leafMin, nodeMin, dims * sizeof(float));
    memcpy(leafMax, nodeMax, dims * sizeof(float));

    char msg[128];
    snprintf(msg, sizeof(msg), "Leaf bounding box computed: leaf at depth %d", pathLen);
    logMessage(msg, INFO);
}

static void rangeQueryOnLeaf(KDNode* leaf, RangeQuery* query, RangeResult* result)
{
    if(!leaf || leaf->type != LEAF || !query || !result)
    {
        logMessage("Invalid parameters for leaf range query", ERROR);
        return;
    }

    size_t pointsFound = 0;

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
    {
        if(pointInRange(&leaf->data.leaf.points[i], query->minBounds, query->maxBounds))
        {
            if(result->count >= result->capacity)
            {
                result->capacity = result->capacity == 0 ? 64 : result->capacity * 2;
                result->points = (point**)realloc(result->points, result->capacity * sizeof(point*));

                if(!result->points)
                {
                    logMessage("Failed to reallocate range result points", ERROR);
                    return;
                }
            }

            result->points[result->count++] = &leaf->data.leaf.points[i];
            pointsFound++;
        }
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Leaf range query: found %zu points out of %zu", pointsFound, leaf->data.leaf.pointsCount);
    logMessage(msg, INFO);
}

static void collectPointsFromSubtree(KDNode* node, RangeResult* result)
{
    if(!node)
        return;

    if(node->type == LEAF)
    {
        for(size_t i = 0; i < node->data.leaf.pointsCount; ++i)
        {
            if(result->count >= result->capacity)
            {
                result->capacity = result->capacity == 0 ? 64 : result->capacity * 2;
                result->points = (point**)realloc(result->points, result->capacity * sizeof(point*));

                if(!result->points)
                {
                    logMessage("Failed to reallocate range result points", ERROR);
                    return;
                }
            }

            result->points[result->count++] = &node->data.leaf.points[i];
        }
    }
    else
    {
        collectPointsFromSubtree(node->data.internal.left, result);
        collectPointsFromSubtree(node->data.internal.right, result);
    }
}

static void rangeQuerySubtree(KDNode* root, RangeQuery* query, RangeResult* result, float* rootMin, float* rootMax)
{
    logMessage("Starting subtree range query", DEBUG);

    if(!root || !query || !result)
    {
        logMessage("Invalid parameters for subtree range query", ERROR);
        return;
    }

    RangeStackEntry* stack = NULL;
    uint8_t dims = getDimensions();

    pushEntry(&stack, root, rootMin, rootMax);

    while(stack)
    {
        RangeStackEntry* entry = popEntry(&stack);

        if(!nodeIntersectsRange(entry->node, query->minBounds, query->maxBounds, entry->nodeMin, entry->nodeMax))
        {
            logMessage("Node does not intersect range, skipping", DEBUG);
            freeEntry(entry);
            continue;
        }

        if(nodeFullyContained(entry->node, entry->nodeMin, entry->nodeMax, query->minBounds, query->maxBounds))
        {
            logMessage("Node fully contained, collecting all points", DEBUG);
            collectPointsFromSubtree(entry->node, result);
            freeEntry(entry);
            continue;
        }

        if(entry->node->type == LEAF)
        {
            logMessage("Processing leaf node", DEBUG);
            rangeQueryOnLeaf(entry->node, query, result);
            freeEntry(entry);
            continue;
        }

        uint8_t splitDim = entry->node->data.internal.splitDim;
        float splitValue = entry->node->data.internal.splitValue;

        float leftMin[dims], leftMax[dims];
        float rightMin[dims], rightMax[dims];

        if(entry->node->data.internal.left)
        {
            expandBoundingBox(entry->node->data.internal.left, leftMin, leftMax, entry->nodeMin, entry->nodeMax, splitDim, splitValue, true);
            pushEntry(&stack, entry->node->data.internal.left, leftMin, leftMax);
        }

        if(entry->node->data.internal.right)
        {
            expandBoundingBox(entry->node->data.internal.right, rightMin, rightMax, entry->nodeMin, entry->nodeMax, splitDim, splitValue, false);
            pushEntry(&stack, entry->node->data.internal.right, rightMin, rightMax);
        }

        freeEntry(entry);
    }

    logMessage("Subtree range query completed", DEBUG);
}

static void exploreSiblingSubtrees(KDNode* leaf, RangeQuery* query, RangeResult* result, float* leafMin, float* leafMax)
{
    logMessage("Exploring sibling subtrees", DEBUG);

    if(!leaf || leaf->type != LEAF)
        return;

    uint8_t dims = getDimensions();
    KDNode* current = leaf;
    KDNode* parent = leaf->parent;

    float currentMin[dims], currentMax[dims];
    memcpy(currentMin, leafMin, dims * sizeof(float));
    memcpy(currentMax, leafMax, dims * sizeof(float));

    while(parent)
    {
        bool isLeftChild = (parent->data.internal.left == current);
        KDNode* sibling = isLeftChild ? parent->data.internal.right : parent->data.internal.left;

        if(sibling)
        {
            logMessage("Processing sibling node", DEBUG);

            float parentMin[dims], parentMax[dims];

            for(uint8_t d = 0; d < dims; ++d)
            {
                parentMin[d] = currentMin[d];
                parentMax[d] = currentMax[d];
            }

            uint8_t splitDim = parent->data.internal.splitDim;
            parentMin[splitDim] = getMinCoord();
            parentMax[splitDim] = getMaxCoord();

            float siblingMin[dims], siblingMax[dims];

            memcpy(siblingMin, parentMin, dims * sizeof(float));
            memcpy(siblingMax, parentMax, dims * sizeof(float));

            if(isLeftChild)
                siblingMin[splitDim] = parent->data.internal.splitValue;
            else
                siblingMax[splitDim] = parent->data.internal.splitValue;

            if(nodeIntersectsRange(sibling, query->minBounds, query->maxBounds, siblingMin, siblingMax))
                rangeQuerySubtree(sibling, query, result, siblingMin, siblingMax);
        }

        if(parent->parent)
        {
            uint8_t splitDim = parent->data.internal.splitDim;
            currentMin[splitDim] = getMinCoord();
            currentMax[splitDim] = getMaxCoord();
        }

        current = parent;
        parent = parent->parent;
    }

    logMessage("Sibling subtrees exploration completed", DEBUG);
}

RangeBatch* initRangeBatch(size_t batchSize)
{
    logMessage("Initializing range query batch", DEBUG);

    if(batchSize == 0)
    {
        logMessage("Invalid batch size: 0", ERROR);
        return NULL;
    }

    RangeBatch* batch = (RangeBatch*)malloc(sizeof(RangeBatch));
    if(!batch)
    {
        logMessage("Failed to allocate range batch", ERROR);
        return NULL;
    }

    batch->queries = (RangeQuery*)calloc(batchSize, sizeof(RangeQuery));
    batch->results = (RangeResult*)calloc(batchSize, sizeof(RangeResult));
    batch->size = batchSize;

    if(!batch->queries || !batch->results)
    {
        logMessage("Failed to allocate queries or results arrays", ERROR);
        free(batch->queries);
        free(batch->results);
        free(batch);
        return NULL;
    }

    uint8_t dims = getDimensions();

    for(size_t i = 0; i < batchSize; ++i)
    {
        batch->queries[i].minBounds = (float*)malloc(dims * sizeof(float));
        batch->queries[i].maxBounds = (float*)malloc(dims * sizeof(float));

        if(!batch->queries[i].minBounds || !batch->queries[i].maxBounds)
        {
            logMessage("Failed to allocate query bounds", ERROR);
            for(size_t j = 0; j <= i; ++j)
            {
                free(batch->queries[j].minBounds);
                free(batch->queries[j].maxBounds);
            }

            free(batch->queries);
            free(batch->results);
            free(batch);

            return NULL;
        }

        batch->results[i].points = NULL;
        batch->results[i].count = 0;
        batch->results[i].capacity = 0;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Range query batch initialized with size %zu", batchSize);
    logMessage(msg, INFO);

    return batch;
}

void freeRangeBatch(RangeBatch* batch)
{
    logMessage("Freeing range query batch", DEBUG);

    if(!batch)
        return;

    for(size_t i = 0; i < batch->size; ++i)
    {
        free(batch->queries[i].minBounds);
        free(batch->queries[i].maxBounds);

        if(batch->results[i].points)
            free(batch->results[i].points);
    }

    free(batch->queries);
    free(batch->results);
    free(batch);

    logMessage("Range query batch freed", INFO);
}

bool pointInRange(point* p, float* minBounds, float* maxBounds)
{
    if(!p || !minBounds || !maxBounds)
        return false;

    uint8_t dims = getDimensions();

    for(uint8_t d = 0; d < dims; ++d)
    {
        if(p->coords[d] < minBounds[d] - 1e-9f || p->coords[d] > maxBounds[d] + 1e-9f)
            return false;
    }

    return true;
}

bool nodeIntersectsRange(KDNode* node, float* minBounds, float* maxBounds, float* nodeMin, float* nodeMax)
{
    if(!node || !minBounds || !maxBounds || !nodeMin || !nodeMax)
        return false;

    uint8_t dims = getDimensions();

    for(uint8_t d = 0; d < dims; ++d)
        if(nodeMax[d] < minBounds[d] - 1e-9f || nodeMin[d] > maxBounds[d] + 1e-9f)
            return false;

    return true;
}

bool nodeFullyContained(KDNode* node, float* nodeMin, float* nodeMax, float* minBounds, float* maxBounds)
{
    uint8_t dims = getDimensions();

    for(uint8_t d = 0; d < dims; ++d)
        if(nodeMin[d] < minBounds[d] - 1e-9f || nodeMax[d] > maxBounds[d] + 1e-9f)
            return false;

    return true;
}

bool batchRangeQuery(RangeBatch* batch)
{
    logMessage("Starting batch range query with push-pull", DEBUG);

    if(!batch || batch->size == 0)
    {
        logMessage("Invalid batch or empty batch", ERROR);
        return false;
    }

    Data* data = getData();
    if(!data || !data->tree || !data->tree->root)
    {
        logMessage("Tree not initialized", ERROR);
        return false;
    }

    KDNode* root = data->tree->root;
    uint8_t dims = getDimensions();
    size_t batchSize = batch->size;

    char msg[128];
    snprintf(msg, sizeof(msg), "Processing batch of %zu queries", batchSize);
    logMessage(msg, INFO);

    point** queryCenters = (point**)malloc(batchSize * sizeof(point*));
    if(!queryCenters)
    {
        logMessage("Failed to allocate query centers", ERROR);
        return false;
    }

    for(size_t i = 0; i < batchSize; ++i)
    {
        queryCenters[i] = (point*)malloc(sizeof(point));
        if(!queryCenters[i])
        {
            logMessage("Failed to allocate query center point", ERROR);
            for(size_t j = 0; j < i; ++j)
                free(queryCenters[j]);

            free(queryCenters);

            return false;
        }

        queryCenters[i]->coords = (float*)malloc(dims * sizeof(float));
        for(uint8_t d = 0; d < dims; ++d)
            queryCenters[i]->coords[d] = (batch->queries[i].minBounds[d] + batch->queries[i].maxBounds[d]) / 2.0f;
    }

    SearchBatch* searchBatch = initSearchBatch(queryCenters, batchSize);
    if(!searchBatch)
    {
        logMessage("Failed to initialize search batch", ERROR);
        for(size_t i = 0; i < batchSize; ++i)
            free(queryCenters[i]);

        free(queryCenters);

        return false;
    }

    searchBatch = leafSearch(searchBatch);

    #pragma omp parallel for schedule(dynamic)
    for(size_t i = 0; i < batchSize; ++i)
    {
        KDNode* startLeaf = searchBatch->results[i];

        if(!startLeaf || startLeaf->type != LEAF)
        {
            logMessage("No starting leaf found, using root", DEBUG);
            float rootMin[dims], rootMax[dims];
            initializeRootBoundingBox(rootMin, rootMax);
            rangeQuerySubtree(root, &batch->queries[i], &batch->results[i], rootMin, rootMax);
            continue;
        }

        float leafMin[dims], leafMax[dims];
        computeLeafBoundingBox(startLeaf, leafMin, leafMax);

        if(nodeIntersectsRange(startLeaf, batch->queries[i].minBounds, batch->queries[i].maxBounds, leafMin, leafMax))
            rangeQueryOnLeaf(startLeaf, &batch->queries[i], &batch->results[i]);

        exploreSiblingSubtrees(startLeaf, &batch->queries[i], &batch->results[i], leafMin, leafMax);
    }

    freeSearchBatch(searchBatch);
    for(size_t i = 0; i < batchSize; ++i)
        free(queryCenters[i]);
    free(queryCenters);

    size_t totalPoints = 0;
    for(size_t i = 0; i < batchSize; ++i)
        totalPoints += batch->results[i].count;

    snprintf(msg, sizeof(msg), "Batch range query completed: %zu total points returned", totalPoints);
    logMessage(msg, INFO);

    return true;
}

void freeRangeResult(RangeResult* result)
{
    if(!result)
        return;

    if(result->points)
        free(result->points);

    free(result);
    logMessage("Range result freed", DEBUG);
}

RangeResult* rangeQuery(float* minBounds, float* maxBounds)
{
    logMessage("Executing single range query", DEBUG);

    if(!minBounds || !maxBounds)
    {
        logMessage("Invalid bounds provided", ERROR);
        return NULL;
    }

    RangeBatch* batch = initRangeBatch(1);
    if(!batch)
        return NULL;

    uint8_t dims = getDimensions();
    memcpy(batch->queries[0].minBounds, minBounds, dims * sizeof(float));
    memcpy(batch->queries[0].maxBounds, maxBounds, dims * sizeof(float));

    if(!batchRangeQuery(batch))
    {
        logMessage("Batch range query failed", ERROR);
        freeRangeBatch(batch);
        return NULL;
    }

    RangeResult* result = (RangeResult*)malloc(sizeof(RangeResult));
    if(!result)
    {
        logMessage("Failed to allocate result", ERROR);
        freeRangeBatch(batch);
        return NULL;
    }

    result->points = batch->results[0].points;
    result->count = batch->results[0].count;
    result->capacity = batch->results[0].capacity;

    batch->results[0].points = NULL;
    batch->results[0].count = 0;
    batch->results[0].capacity = 0;

    freeRangeBatch(batch);

    char msg[128];
    snprintf(msg, sizeof(msg), "Single range query completed: %zu points found", result->count);
    logMessage(msg, INFO);

    return result;
}

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dpu.h>
#include <dpu_types.h>

#include "kdTree/search.h"
#include "kdTree/build.h"
#include "kdTree/utils.h"
#include "environment/constants.h"
#include "environment/init.h"

typedef struct NodeGroup
{
    KDNode* node;
    uint32_t* indices;
    uint32_t count;
} NodeGroup;



static uint32_t findDpuForNode(KDNode* node)
{
    Data* data = getData();
    NodeLocationMap* map = data->map;

    if(!map)
        return (uint32_t)((uintptr_t)node % getConfig()->nPim);

    for(size_t i = 0; i < map->count; ++i)
        if(map->nodes[i] == node)
            return map->dpuIds[i];

    return (uint32_t)((uintptr_t)node % getConfig()->nPim);
}

static uint64_t getDpuAddress(KDNode* node, uint32_t dpuId)
{
    Data* data = getData();
    NodeLocationMap* map = data->map;

    if(!map)
        return 0;

    for(size_t i = 0; i < map->count; ++i)
        if(map->nodes[i] == node && map->dpuIds[i] == dpuId)
            return map->dpuAddresses[i];

    return 0;
}

void initNodeLocationMap()
{
    Data* data = getData();

    if(!data->map)
    {
        data->map = malloc(sizeof(NodeLocationMap));
        if(!data->map)
            return;
    }

    data->map->capacity = 128;
    data->map->count = 0;
    data->map->nodes = calloc(data->map->capacity, sizeof(KDNode*));
    data->map->dpuIds = calloc(data->map->capacity, sizeof(uint32_t));
    data->map->dpuAddresses = calloc(data->map->capacity, sizeof(uint64_t));
}

void freeNodeLocationMap()
{
    Data* data = getData();

    if(!data->map)
        return;

    free(data->map->nodes);
    free(data->map->dpuIds);
    free(data->map->dpuAddresses);
    free(data->map);
    data->map = NULL;
}

void registerNodeLocation(KDNode* node, uint32_t dpuId, uint64_t dpuAddr)
{
    Data* data = getData();

    if(!data->map)
    {
        initNodeLocationMap();
        if(!data->map)
            return;
    }

    if(data->map->count >= data->map->capacity)
    {
        data->map->capacity *= 2;
        data->map->nodes = realloc(data->map->nodes, data->map->capacity * sizeof(KDNode*));
        data->map->dpuIds = realloc(data->map->dpuIds, data->map->capacity * sizeof(uint32_t));
        data->map->dpuAddresses = realloc(data->map->dpuAddresses, data->map->capacity * sizeof(uint64_t));
    }

    data->map->nodes[data->map->count] = node;
    data->map->dpuIds[data->map->count] = dpuId;
    data->map->dpuAddresses[data->map->count] = dpuAddr;
    data->map->count++;
}

KDNode* resolveNodeLocation(uint64_t address, uint32_t dpuId)
{
    Data* data = getData();
    NodeLocationMap* map = data->map;

    if(!map)
        return NULL;

    for(size_t i = 0; i < map->count; ++i)
        if(map->dpuAddresses[i] == address && map->dpuIds[i] == dpuId)
            return map->nodes[i];

    return NULL;
}

SearchBatch* initSearchBatch(point** queries, size_t size)
{
    SearchBatch* batch = (SearchBatch*)malloc(sizeof(SearchBatch));
    if(!batch)
        return NULL;

    batch->queries = queries;
    batch->size = size;
    batch->results = (SearchResult*)malloc(size * sizeof(SearchResult));

    if(!batch->results)
    {
        free(batch);
        return NULL;
    }

    for(size_t i = 0; i < size; ++i)
    {
        batch->results[i].leaf = NULL;
        batch->results[i].pathLength = 0;
        batch->results[i].groupsTraversed = 0;
        batch->results[i].crossGroupTransfers = 0;
        batch->results[i].dpuAccesses = 0;
    }

    return batch;
}

void freeSearchBatch(SearchBatch* batch)
{
    if(!batch)
        return;

    if(batch->results)
        free(batch->results);

    free(batch);
}

static uint16_t calculateGroupHeight(uint8_t groupId)
{
    double value = getConfig()->nPim;

    for(uint8_t i = 0; i < groupId; ++i)
    {
        value = log2(value);
        if(value <= 1.0)
        {
            value = 1.0;
            break;
        }
    }

    return (uint16_t)(value + 1);
}

static uint8_t calculateNumGroups()
{
    uint8_t groups = 0;
    double value = getConfig()->nPim;

    while(value > 1.0)
    {
        value = log2(value);
        ++groups;
    }

    return groups;
}

PushPullContext* initPushPullContext()
{
    PushPullContext* context = (PushPullContext*)malloc(sizeof(PushPullContext));
    if(!context)
        return NULL;

    context->nodeVisits = 0;
    context->fanout = 2;
    context->forcePull = false;

    context->numGroups = calculateNumGroups(getConfig()->nPim);

    context->groupThresholds = (uint32_t*)malloc(context->numGroups * sizeof(uint32_t));
    context->groupAccessCounts = (uint32_t*)calloc(context->numGroups, sizeof(uint32_t));
    context->groupPullDecision = (bool*)calloc(context->numGroups, sizeof(bool));
    if(!context->groupThresholds || !context->groupAccessCounts || !context->groupPullDecision)
    {
        free(context->groupThresholds);
        free(context->groupAccessCounts);
        free(context->groupPullDecision);

        free(context);
        return NULL;
    }

    for(uint8_t g = 0; g < context->numGroups; ++g)
        context->groupThresholds[g] = context->fanout * calculateGroupHeight(g);

    return context;
}

void updatePushPullContext(PushPullContext* context)
{
    if(!context)
        return;

    for(uint8_t g = 0; g < context->numGroups; ++g)
        context->groupPullDecision[g] = (context->groupAccessCounts[g] >  context->groupThresholds[g]);
}

void freePushPullContext(PushPullContext* context)
{
    if(!context)
        return;

    if(context->groupThresholds)
        free(context->groupThresholds);
    if(context->groupAccessCounts)
        free(context->groupAccessCounts);
    if(context->groupPullDecision)
        free(context->groupPullDecision);

    free(context);
}

bool shouldPull(KDNode* node, uint32_t accessCount, uint8_t groupId, PushPullContext* context)
{
    if(!node || !context)
        return false;

    if(context->forcePull)
        return true;

    if(node->type == LEAF)
        return false;

    if(groupId < context->numGroups)
        return accessCount > context->groupThresholds[groupId];

    return accessCount > context->fanout * 4;
}

void pushToPim(point** queries, uint32_t count, KDNode* startNode, uint8_t groupId, SearchResult* results)
{
    if(count == 0 || !queries || !startNode)
        return;

    struct dpu_set_t dpu;
    struct dpu_set_t set;
    uint32_t found = 0;

    DPU_ASSERT(dpu_alloc(getConfig()->nPim, NULL, &set));
    DPU_ASSERT(dpu_load(set, "task", NULL));

    uint32_t dpuId = findDpuForNode(startNode);
    uint64_t nodeAddr = getDpuAddress(startNode, dpuId);


    DPU_FOREACH(set, dpu)
    {
        if(found++ == dpuId)
        {
            size_t querySize = count * getConfig()->dimensions * sizeof(float);
            float* queryData = (float*)malloc(querySize);

            for(uint32_t i = 0; i < count; i++)
                memcpy(&queryData[i * getConfig()->dimensions], queries[i]->coords, getConfig()->dimensions * sizeof(float));

            uint64_t args[4] = { nodeAddr, count, getConfig()->dimensions, groupId };

            DPU_ASSERT(dpu_copy_to(dpu, "DPU_ARGS", 0, args, sizeof(args)));
            DPU_ASSERT(dpu_copy_to(dpu, "QUERY_BATCH", 0, queryData, querySize));

            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));

            size_t resultsSize;
            DPU_ASSERT(dpu_copy_from(dpu, "RESULTS_SIZE", 0, &resultsSize, sizeof(size_t)));

            if(resultsSize > 0)
            {
                uint8_t* serialized = (uint8_t*)malloc(resultsSize);
                DPU_ASSERT(dpu_copy_from(dpu, "RESULTS", 0, serialized, resultsSize));

                uint8_t* ptr = serialized;
                uint8_t* end = ptr + resultsSize;

                for(size_t i = 0; i < count && ptr < end; ++i)
                {
                    uint64_t leafAddr;
                    memcpy(&leafAddr, ptr, sizeof(uint64_t));
                    ptr += sizeof(uint64_t);

                    results[i].leaf = resolveNodeLocation(leafAddr, dpuId);

                    memcpy(&results[i].pathLength, ptr, sizeof(uint16_t));
                    ptr += sizeof(uint16_t);

                    results[i].groupsTraversed++;
                    results[i].crossGroupTransfers++;
                    results[i].dpuAccesses++;
                }

                free(serialized);
            }

            free(queryData);
            break;
        }
    }

    DPU_ASSERT(dpu_free(set));
}

KDNode** pullToCpu(KDNode* node, point** queries, uint32_t count, PushPullContext* context)
{
    if(!node || !queries || count == 0 || !context)
        return NULL;

    uint32_t dpuId = findDpuForNode(node);
    uint64_t nodeAddr = getDpuAddress(node, dpuId);

    struct dpu_set_t set;

    DPU_ASSERT(dpu_alloc(getConfig()->nPim, NULL, &set));
    DPU_ASSERT(dpu_load(set, "task", NULL));

    KDNode** nextNodes = (KDNode**)calloc(count, sizeof(KDNode*));
    if(!nextNodes)
        return NULL;

    if(nodeAddr == 0)
    {
        for(uint32_t i = 0; i < count; ++i)
        {
            KDNode* current = node;
            point* q = queries[i];

            while(current && current->type == INTERNAL)
            {
                if(q->coords[current->data.internal.splitDim] <
                   current->data.internal.splitValue)
                    current = current->data.internal.left;
                else
                    current = current->data.internal.right;
            }
            nextNodes[i] = current;
        }
        return nextNodes;
    }

    struct dpu_set_t dpu;
    uint32_t found = 0;

    DPU_FOREACH(set, dpu)
    {
        if(found++ == dpuId)
        {
            uint64_t request = nodeAddr;
            DPU_ASSERT(dpu_copy_to(dpu, "PULL_REQUEST", 0, &request, sizeof(uint64_t)));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));

            uint64_t subtreeSize;
            DPU_ASSERT(dpu_copy_from(dpu, "SUBTREE_SIZE", 0, &subtreeSize, sizeof(uint64_t)));

            if(subtreeSize > 0)
            {
                uint8_t* serialized = (uint8_t*)malloc(subtreeSize);
                DPU_ASSERT(dpu_copy_from(dpu, "SUBTREE_DATA", 0, serialized, subtreeSize));

                KDNode* pulledNode = deserializeTree(serialized, subtreeSize);

                if(pulledNode)
                {
                    for(uint32_t i = 0; i < count; ++i)
                    {
                        KDNode* current = pulledNode;
                        point* q = queries[i];

                        while(current && current->type == INTERNAL)
                        {
                            if(q->coords[current->data.internal.splitDim] < current->data.internal.splitValue)
                                current = current->data.internal.left;
                            else
                                current = current->data.internal.right;
                        }

                        nextNodes[i] = current;
                    }
                }

                free(serialized);
            }

            break;
        }
    }

    DPU_ASSERT(dpu_free(set));

    return nextNodes;
}


void searchGroup0(SearchBatch* batch, KDTree* tree, KDNode*** group1Roots)
{
    if(!batch || !tree)
        return;

    struct dpu_set_t set;

    DPU_ASSERT(dpu_alloc(getConfig()->nPim, NULL, &set));
    DPU_ASSERT(dpu_load(set, "task", NULL));

    uint32_t queriesPerPim = batch->size / getConfig()->nPim;
    uint32_t remainder = batch->size % getConfig()->nPim;

    *group1Roots = (KDNode**)calloc(batch->size, sizeof(KDNode*));
    if(!*group1Roots)
        return;

    for(size_t pimId = 0; pimId < getConfig()->nPim; ++pimId)
    {
        uint32_t startIndex = pimId * queriesPerPim + (pimId < remainder ? pimId : remainder);
        uint32_t endIndex = startIndex + queriesPerPim + (pimId < remainder ? 1 : 0);
        uint32_t localCount = endIndex - startIndex;

        if(localCount == 0)
            continue;

        size_t querySize = localCount * getConfig()->dimensions * sizeof(float);
        float* queryData = (float*)malloc(querySize);

        for(uint32_t i = 0; i < localCount; ++i)
            memcpy(&queryData[i * getConfig()->dimensions], batch->queries[startIndex + i]->coords, getConfig()->dimensions * sizeof(float));

        struct dpu_set_t dpu;
        uint32_t found = 0;

        DPU_FOREACH(set, dpu)
        {
            if(found++ == pimId)
            {
                uint64_t args[3] = { (uint64_t)(uintptr_t)tree->root, localCount, getConfig()->dimensions };

                DPU_ASSERT(dpu_copy_to(dpu, "GROUP0_ARGS", 0, args, sizeof(args)));
                DPU_ASSERT(dpu_copy_to(dpu, "GROUP0_QUERIES", 0, queryData, querySize));
            }
        }

        free(queryData);
    }

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    for(uint32_t pimId = 0; pimId < getConfig()->nPim; ++pimId)
    {
        uint32_t startIndex = pimId * queriesPerPim + (pimId < remainder ? pimId : remainder);
        uint32_t endIndex = startIndex + queriesPerPim + (pimId < remainder ? 1 : 0);
        uint32_t localCount = endIndex - startIndex;

        if(localCount == 0)
            continue;

        struct dpu_set_t dpu;
        uint32_t found = 0;

        DPU_FOREACH(set, dpu)
        {
            if(found++ == pimId)
            {
                size_t resultsSize;
                DPU_ASSERT(dpu_copy_from(dpu, "GROUP0_RESULTS_SIZE", 0, &resultsSize, sizeof(size_t)));

                if(resultsSize > 0)
                {
                    uint64_t* g1RootAddrs = (uint64_t*)malloc(resultsSize);
                    DPU_ASSERT(dpu_copy_from(dpu, "GROUP0_RESULTS", 0, g1RootAddrs, resultsSize));

                    uint32_t resultCount = resultsSize / sizeof(uint64_t);
                    for(uint32_t i = 0; i < resultCount && i < localCount; ++i)
                    {
                        (*group1Roots)[startIndex + i] = (KDNode*)(uintptr_t)g1RootAddrs[i];

                        batch->results[startIndex + i].dpuAccesses++;
                        batch->results[startIndex + i].groupsTraversed++;
                        batch->results[startIndex + i].pathLength++;
                    }

                    free(g1RootAddrs);
                }
                break;
            }
        }
    }

    DPU_ASSERT(dpu_free(set));
}


void traverseGroupWithPushPull(SearchBatch* batch, KDNode** currentNodes, uint8_t groupId, PushPullContext* context)
{
    if(!batch || !currentNodes || !context)
        return;

    uint32_t* nodeCounts = (uint32_t*)calloc(batch->size, sizeof(uint32_t));
    uint32_t uniqueNodes = 0;

    for(size_t i = 0; i < batch->size; ++i)
    {
        if(!currentNodes[i] || currentNodes[i]->type == LEAF)
            continue;

        bool found = false;
        for(size_t j = 0; j < i; ++j)
        {
            if(currentNodes[j] == currentNodes[i])
            {
                nodeCounts[j]++;
                found = true;
                break;
            }
        }

        if(!found)
        {
            nodeCounts[i] = 1;
            uniqueNodes++;
        }
    }

    if(uniqueNodes == 0)
    {
        free(nodeCounts);
        return;
    }

    NodeGroup* groups = (NodeGroup*)calloc(uniqueNodes, sizeof(NodeGroup));
    uint32_t groupIndex = 0;

    for(size_t i = 0; i < batch->size; ++i)
    {
        if(nodeCounts[i] > 0)
        {
            groups[groupIndex].node = currentNodes[i];
            groups[groupIndex].count = nodeCounts[i];
            groups[groupIndex].indices = (uint32_t*)malloc(nodeCounts[i] * sizeof(uint32_t));

            uint32_t index= 0;
            for(size_t j = i; j < batch->size; ++j)
            {
                if(currentNodes[j] == currentNodes[i])
                    groups[groupIndex].indices[index++] = j;
            }
            groupIndex++;
        }
    }

    for(uint32_t g = 0; g < uniqueNodes; ++g)
        context->groupAccessCounts[groupId] += groups[g].count;

    #pragma omp parallel for
    for(uint32_t g = 0; g < uniqueNodes; ++g)
    {
        KDNode* node = groups[g].node;
        uint32_t accessCount = groups[g].count;

        point** nodeQueries = (point**)malloc(accessCount * sizeof(point*));
        for(uint32_t i = 0; i < accessCount; ++i)
            nodeQueries[i] = batch->queries[groups[g].indices[i]];

        if(shouldPull(node, accessCount, groupId, context))
        {
            KDNode** nextNodes = pullToCpu(node, nodeQueries, accessCount, context);

            if(nextNodes)
            {
                for(uint32_t i = 0; i < accessCount; ++i)
                {
                    uint32_t index = groups[g].indices[i];
                    currentNodes[index] = nextNodes[i];

                    batch->results[index].pathLength++;
                    batch->results[index].crossGroupTransfers++;
                    batch->results[index].dpuAccesses++;

                    if(nextNodes[i] && nextNodes[i]->type == LEAF)
                        batch->results[index].leaf = nextNodes[i];
                }
                free(nextNodes);
            }
        }
        else
        {
            SearchResult* pushResults = (SearchResult*)calloc(accessCount, sizeof(SearchResult));

            pushToPim(nodeQueries, accessCount, node, groupId, pushResults);

            for(uint32_t i = 0; i < accessCount; ++i)
            {
                uint32_t index = groups[g].indices[i];

                if(pushResults[i].leaf)
                {
                    currentNodes[index] = pushResults[i].leaf;
                    batch->results[index].leaf = pushResults[i].leaf;
                }

                batch->results[index].pathLength += pushResults[i].pathLength;
                batch->results[index].groupsTraversed += pushResults[i].groupsTraversed;
                batch->results[index].crossGroupTransfers += pushResults[i].crossGroupTransfers;
                batch->results[index].dpuAccesses += pushResults[i].dpuAccesses;
            }

            free(pushResults);
        }

        free(nodeQueries);
    }

    for(uint32_t g = 0; g < uniqueNodes; ++g)
        free(groups[g].indices);

    free(groups);
    free(nodeCounts);

    updatePushPullContext(context);
}

SearchBatch* leafSearch(SearchBatch* batch, KDTree* tree)
{
    if(!batch || !tree)
        return NULL;

    PushPullContext* context = initPushPullContext();
    if(!context)
        return batch;

    KDNode** group1Roots = NULL;
    searchGroup0(batch, tree, &group1Roots);
    if(!group1Roots)
    {
        freePushPullContext(context);
        return batch;
    }

    KDNode** currentNodes = (KDNode**)malloc(batch->size * sizeof(KDNode*));
    if(!currentNodes)
    {
        free(group1Roots);
        freePushPullContext(context);
        return batch;
    }

    for(size_t i = 0; i < batch->size; ++i)
        currentNodes[i] = group1Roots[i];

    for(uint8_t groupId = 1; groupId < context->numGroups; ++groupId)
    {
        bool allLeaves = true;

        for(size_t i = 0; i < batch->size && allLeaves; ++i)
            if(currentNodes[i] && currentNodes[i]->type != LEAF)
                allLeaves = false;

        if(allLeaves)
            break;

        traverseGroupWithPushPull(batch, currentNodes, groupId, context);
    }

    for(size_t i = 0; i < batch->size; ++i)
        if(currentNodes[i] && currentNodes[i]->type == LEAF && !batch->results[i].leaf)
            batch->results[i].leaf = currentNodes[i];

    free(currentNodes);
    free(group1Roots);
    freePushPullContext(context);

    return batch;
}

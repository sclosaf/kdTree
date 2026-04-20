#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dpu.h>
#include <dpu_types.h>
#include <omp.h>

#include "host/kdTree/distribute.h"
#include "host/kdTree/build.h"
#include "host/kdTree/serialization.h"
#include "host/kdTree/utils.h"

#include "host/loader/dispatcher.h"
#include "host/environment/init.h"
#include "host/management/logging.h"

void initNodeLocationMap()
{
    logMessage("Initializing node location map", DEBUG);

    Data* data = getData();

    if(!data->map)
    {
        data->map = malloc(sizeof(NodeLocationMap));
        if(!data->map)
        {
            logMessage("Failed to allocate node location map", ERROR);
            return;
        }
    }

    data->map->capacity = 128;
    data->map->count = 0;
    data->map->nodes = calloc(data->map->capacity, sizeof(KDNode*));
    data->map->dpuIds = calloc(data->map->capacity, sizeof(uint32_t));
    data->map->dpuAddresses = calloc(data->map->capacity, sizeof(uint64_t));

    logMessage("Node location map initialized with capacity 128", INFO);
}

void registerNodeLocation(KDNode* node, uint32_t dpuId, uint64_t dpuAddr)
{
    logMessage("Registering node location", DEBUG);

    Data* data = getData();

    if(!data->map)
    {
        logMessage("Map not initialized, initializing now", DEBUG);
        initNodeLocationMap();
        if(!data->map)
        {
            logMessage("Failed to initialize node location map", ERROR);
            return;
        }
    }

    if(data->map->count >= data->map->capacity)
    {
        data->map->capacity *= 2;
        data->map->nodes = realloc(data->map->nodes, data->map->capacity * sizeof(KDNode*));
        data->map->dpuIds = realloc(data->map->dpuIds, data->map->capacity * sizeof(uint32_t));
        data->map->dpuAddresses = realloc(data->map->dpuAddresses, data->map->capacity * sizeof(uint64_t));

        char msg[256];
        snprintf(msg, sizeof(msg), "Node location map reallocated to capacity %zu", data->map->capacity);
        logMessage(msg, DEBUG);
    }

    data->map->nodes[data->map->count] = node;
    data->map->dpuIds[data->map->count] = dpuId;
    data->map->dpuAddresses[data->map->count] = dpuAddr;
    ++data->map->count;

    char msg[256];
    snprintf(msg, sizeof(msg), "Node registered: dpuId=%u, dpuAddr=0x%lx, total entries=%zu", dpuId, dpuAddr, data->map->count);
    logMessage(msg, INFO);
}

KDNode* resolveNodeLocation(uint64_t address, uint32_t dpuId)
{
    logMessage("Resolving node location", DEBUG);

    Data* data = getData();
    NodeLocationMap* map = data->map;

    if(!map)
    {
        logMessage("Node location map not initialized", ERROR);
        return NULL;
    }

    for(size_t i = 0; i < map->count; ++i)
        if(map->dpuAddresses[i] == address && map->dpuIds[i] == dpuId)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Node resolved: dpuId=%u, address=0x%lx", dpuId, address);
            logMessage(msg, INFO);
            return map->nodes[i];
        }

    char msg[256];
    snprintf(msg, sizeof(msg), "No node found for dpuId=%u, address=0x%lx", dpuId, address);
    logMessage(msg, ERROR);

    return NULL;
}

uint64_t getDpuAddress(KDNode* node, uint32_t dpuId)
{
    logMessage("Retrieving DPU address for node", DEBUG);

    Data* data = getData();
    NodeLocationMap* map = data->map;

    if(!map)
    {
        logMessage("Node location map not initialized", ERROR);
        return 0;
    }

    for(size_t i = 0; i < map->count; ++i)
        if(map->nodes[i] == node && map->dpuIds[i] == dpuId)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "DPU address found: 0x%lx for dpuId=%u", map->dpuAddresses[i], dpuId);
            logMessage(msg, INFO);
            return map->dpuAddresses[i];
        }

    char msg[256];
    snprintf(msg, sizeof(msg), "No DPU address found for node on dpuId=%u", dpuId);
    logMessage(msg, ERROR);

    return 0;
}

uint32_t findDpuForNode(KDNode* node)
{
    logMessage("Finding DPU for node", DEBUG);

    Data* data = getData();
    NodeLocationMap* map = data->map;

    if(!map)
    {
        logMessage("Map not initialized, falling back to hash-based DPU assignment", DEBUG);
        return (uint32_t)((uintptr_t)node % getNPim());
    }

    for(size_t i = 0; i < map->count; ++i)
        if(map->nodes[i] == node)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "DPU found in map: dpuId=%u", map->dpuIds[i]);
            logMessage(msg, INFO);
            return map->dpuIds[i];
        }

    uint32_t fallback = (uint32_t)((uintptr_t)node % getNPim());

    char msg[256];
    snprintf(msg, sizeof(msg), "Node not in map, falling back to dpuId=%u", fallback);
    logMessage(msg, DEBUG);

    return fallback;
}

void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity)
{
    logMessage("Collecting node reference", DEBUG);

    if(!node)
        return;

    if(*count >= *capacity)
    {
        *capacity = (*capacity) ? (*capacity) * 2 : 1024;
        *refs = realloc(*refs, (*capacity) * sizeof(KDNode*));
        if(!*refs)
        {
            logMessage("Failed to reallocate node references array", ERROR);
            return;
        }

        char msg[256];
        snprintf(msg, sizeof(msg), "Node references array reallocated to capacity %zu", *capacity);
        logMessage(msg, DEBUG);
    }

    (*refs)[(*count)++] = node;

    if(node->type == INTERNAL)
    {
        collectNodeReferences(node->data.internal.left, refs, count, capacity);
        collectNodeReferences(node->data.internal.right, refs, count, capacity);
    }
    else
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Leaf node collected, total references so far: %zu", *count);
        logMessage(msg, INFO);
    }
}

void scatterReplica(KDNode** subtrees, KDNode* cacheForest, DpuAllocation* alloc)
{
    logMessage("Scattering replicas to DPUs", DEBUG);

    if(!subtrees || !alloc)
    {
        logMessage("Null subtrees or allocation provided", ERROR);
        return;
    }

    uint32_t nPim = getNPim();

    KDGroup** groups = logStarDecompose(cacheForest);
    if(!groups)
    {
        logMessage("Failed to retrieve groups from tree", ERROR);
        return;
    }

    size_t totalReplicas = 0;
    for(int g = 1; groups[g] != NULL; ++g)
        totalReplicas += groups[g]->replicaCount;

    char msg[256];
    snprintf(msg, sizeof(msg), "Total replicas to scatter: %zu across %u DPUs", totalReplicas, nPim);
    logMessage(msg, INFO);

    if(totalReplicas == 0)
    {
        logMessage("No replicas to scatter, returning early", DEBUG);

        for(int g = 0; groups[g] != NULL; ++g)
        {
            if(groups[g]->replicas)
                free(groups[g]->replicas);
            free(groups[g]);
        }

        free(groups);
        return;
    }

    ReplicaDistribution** perDpuDistributions = calloc(nPim, sizeof(ReplicaDistribution*));
    uint32_t* perDpuCounts = calloc(nPim, sizeof(uint32_t));
    uint32_t* perDpuCapacities = calloc(nPim, sizeof(uint32_t));
    uint8_t*** perDpuBuffers = calloc(nPim, sizeof(uint8_t**));
    size_t** perDpuBufferSizes = calloc(nPim, sizeof(size_t*));

    if(!perDpuDistributions || !perDpuCounts || !perDpuCapacities || !perDpuBuffers || !perDpuBufferSizes)
    {
        logMessage("Failed to allocate per-DPU distribution arrays", ERROR);

        for(int g = 0; groups[g] != NULL; ++g)
        {
            if(groups[g]->replicas)
                free(groups[g]->replicas);
            free(groups[g]);
        }

        free(groups);
        free(perDpuDistributions);
        free(perDpuCounts);
        free(perDpuCapacities);
        free(perDpuBuffers);
        free(perDpuBufferSizes);
        return;
    }

    logMessage("Per-DPU distribution arrays allocated", DEBUG);

    for(int g = 1; groups[g] != NULL; ++g)
    {
        KDGroup* group = groups[g];

        for(size_t r = 0; r < group->replicaCount; ++r)
        {
            KDNodeReplica* replica = group->replicas[r];
            if(!replica)
                continue;

            uint32_t targetDpu = rand() % nPim;

            size_t serializedSize;
            uint8_t* serializedData = serializeReplicaTree(replica, &serializedSize);
            if(!serializedData)
            {
                snprintf(msg, sizeof(msg), "Failed to serialize replica %zu of group %d", r, g);
                logMessage(msg, ERROR);
                continue;
            }

            uint32_t offset = allocateOnDpu(alloc, targetDpu, serializedSize);

            snprintf(msg, sizeof(msg), "Replica %zu of group %d serialized (%zu bytes) and allocated at offset %u on DPU %u", r, g, serializedSize, offset, targetDpu);
            logMessage(msg, DEBUG);

            ReplicaDistribution dist = {
                .groupId = (uint8_t)g,
                .replicaIndex = (uint32_t)r,
                .replicaAddress = offset,
                .masterAddress = (uint64_t)(uintptr_t)replica->masterNode,
                .replicaSize = serializedSize
            };

            if(perDpuCounts[targetDpu] >= perDpuCapacities[targetDpu])
            {
                uint32_t newCap = perDpuCapacities[targetDpu] ? perDpuCapacities[targetDpu] * 2 : 16;
                perDpuDistributions[targetDpu] = realloc(perDpuDistributions[targetDpu], newCap * sizeof(ReplicaDistribution));
                perDpuBuffers[targetDpu] = realloc(perDpuBuffers[targetDpu], newCap * sizeof(uint8_t*));
                perDpuBufferSizes[targetDpu] = realloc(perDpuBufferSizes[targetDpu], newCap * sizeof(size_t));
                perDpuCapacities[targetDpu] = newCap;

                snprintf(msg, sizeof(msg), "Per-DPU buffer for DPU %u reallocated to capacity %u", targetDpu, newCap);
                logMessage(msg, DEBUG);
            }

            perDpuDistributions[targetDpu][perDpuCounts[targetDpu]] = dist;
            perDpuBuffers[targetDpu][perDpuCounts[targetDpu]] = serializedData;
            perDpuBufferSizes[targetDpu][perDpuCounts[targetDpu]] = serializedSize;
            ++perDpuCounts[targetDpu];
        }
    }

    logMessage("All replicas packed into per-DPU buffers", DEBUG);

    size_t sketchSize = 0;
    uint8_t* sketchData = NULL;

    if(cacheForest)
    {
        sketchData = serializeTree(cacheForest, &sketchSize);
        snprintf(msg, sizeof(msg), "Cache forest serialized: %zu bytes", sketchSize);
        logMessage(msg, INFO);
    }

    int ret = dispatchDistributeReplicas(perDpuDistributions, perDpuCounts, perDpuBuffers, perDpuBufferSizes, nPim, sketchData, sketchSize, alloc);

    if(ret != 0)
    {
        snprintf(msg, sizeof(msg), "Replica dispatch failed with code %d", ret);
        logMessage(msg, ERROR);
    }
    else
        logMessage("Replicas dispatched to all DPUs successfully", INFO);

    for(uint32_t dpuId = 0; dpuId < nPim; ++dpuId)
    {
        if(perDpuCounts[dpuId] > 0)
        {
            for(uint32_t i = 0; i < perDpuCounts[dpuId]; ++i)
                free(perDpuBuffers[dpuId][i]);

            free(perDpuDistributions[dpuId]);
            free(perDpuBuffers[dpuId]);
            free(perDpuBufferSizes[dpuId]);
        }
    }

    if(sketchData)
        free(sketchData);

    for(int g = 0; groups[g] != NULL; ++g)
    {
        if(groups[g]->replicas)
            free(groups[g]->replicas);
        free(groups[g]);
    }

    free(groups);
    free(perDpuDistributions);
    free(perDpuCounts);
    free(perDpuCapacities);
    free(perDpuBuffers);
    free(perDpuBufferSizes);

    logMessage("Scatter replica cleanup complete", DEBUG);
}

DpuAllocation* createDpuAllocation()
{
    logMessage("Creating DPU allocation structure", DEBUG);

    DpuAllocation* alloc = malloc(sizeof(DpuAllocation));
    if(!alloc)
    {
        logMessage("Failed to allocate DpuAllocation", ERROR);
        return NULL;
    }

    alloc->nextOffset = calloc(getNPim(), sizeof(uint32_t));
    alloc->allocationCount = calloc(getNPim(), sizeof(uint32_t));

    if(!alloc->nextOffset || !alloc->allocationCount)
    {
        logMessage("Failed to allocate DpuAllocation internal arrays", ERROR);
        free(alloc->nextOffset);
        free(alloc->allocationCount);
        free(alloc);
        return NULL;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "DPU allocation created for %u PIMs", getNPim());
    logMessage(msg, INFO);

    return alloc;
}

uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size)
{
    logMessage("Allocating space on DPU", DEBUG);

    uint32_t offset = alloc->nextOffset[dpuId];
    alloc->nextOffset[dpuId] += (uint32_t)size;
    ++alloc->allocationCount[dpuId];

    char msg[256];
    snprintf(msg, sizeof(msg), "Allocated %zu bytes on DPU %u at offset %u (total allocations: %u)", size, dpuId, offset, alloc->allocationCount[dpuId]);
    logMessage(msg, INFO);

    return offset;
}

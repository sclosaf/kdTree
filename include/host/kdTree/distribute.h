#ifndef KDTREE_DISTRIBUTE_H
#define KDTREE_DISTRIBUTE_H

#include <stdint.h>

#include "host/kdTree/types.h"

typedef struct DpuAllocation
{
    uint32_t* nextOffset;
    uint32_t* allocationCount;
} DpuAllocation;

typedef struct NodeLocationMap
{
    KDNode** nodes;
    uint32_t* dpuIds;
    uint64_t* dpuAddresses;
    size_t count;
    size_t capacity;
} NodeLocationMap;

void initNodeLocationMap();
void registerNodeLocation(KDNode* node, uint32_t dpuId, uint64_t dpuAddr);
KDNode* resolveNodeLocation(uint64_t address, uint32_t dpuId);
uint64_t getDpuAddress(KDNode* node, uint32_t dpuId);
uint32_t findDpuForNode(KDNode* node);

KDNode** collectSubtreesFromDpus(size_t* totalNodes);
void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity);
void scatterReplica(KDNode** subtrees, KDNode* cacheForest, DpuAllocation* alloc);
DpuAllocation* createDpuAllocation();
uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size);

#endif

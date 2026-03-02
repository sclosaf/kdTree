#ifndef KDTREE_DISTRIBUTE_H
#define KDTREE_DISTRIBUTE_H

typedef struct ReplicaInfo
{
    uint32_t dpuId;
    uint32_t nodeOffset;
    uint32_t nodeCount;
    uint8_t groupLevel;
} ReplicaInfo;

typedef struct DpuAllocation
{
    uint32_t* nextOffset;
    uint32_t* allocationCount;
    size_t numDpus;
} DpuAllocation;

KDNode** collectSubtreesFromDpus(DPUContext* dpuCtx, size_t P, size_t* totalNodes);
void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity);
void scatterReplica(DPUContext* dpuCtx, KDNode** subtrees, size_t P, size_t totalPoints, KDNode* cacheForest, DpuAllocation* alloc);
void sendSketchToAllDpus(DPUContext* ctx, KDNode* sketch);
DpuAllocation* createDpuAllocation(size_t numDpus);
uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size);

#endif

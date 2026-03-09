#ifndef KDTREE_UTILS_H
#define KDTREE_UTILS_H

#include <stddef.h>
#include <stdint.h>

#include "kdTree/types.h"

#include "environment/init.h"

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

float findMedian(point** points, size_t start, size_t end, uint8_t dim);
uint8_t findSplitDim(point** points, size_t start, size_t end);
int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups);
uint32_t getBucket(KDNode* sketch, point* p);
uint32_t getNodeSize(KDNode* node);
size_t parallelPartition(point** points, size_t start, size_t end, uint8_t dim, float pivot);
int compareByDim(const void* a, const void* b, void* dim);
size_t calculateSubtreeSize(KDNode* node);
uint32_t** computePrefixSum(uint32_t** matrix, size_t rows, size_t cols);

void serializeNodeData(KDNode* node, uint8_t** ptr);
void serializeNodeSize(KDNode* node, size_t* size);
void* serializeTree(KDNode* root, size_t* size);

KDNode* deserializeTree(void* data, size_t size);
KDNode* deserializeNode(uint8_t** ptr, uint8_t* end);

void freeKDTree(KDNode* node);
void freeMatrix(void** matrix, size_t rows);
void freeLeafNode(KDNode* node);
void freeDpuAllocation(DpuAllocation* alloc);

KDNode** collectSubtreesFromDpus(DPUContext* dpuCtx, size_t P, size_t* totalNodes);
void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity);
void scatterReplica(DPUContext* dpuCtx, KDNode** subtrees, size_t P, size_t totalPoints, KDNode* cacheForest, DpuAllocation* alloc);
void sendSketchToAllDpus(DPUContext* ctx, KDNode* sketch);
DpuAllocation* createDpuAllocation(size_t numDpus);
uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size);

#endif

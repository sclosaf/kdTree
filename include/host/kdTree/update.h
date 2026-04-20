#ifndef KDTREE_UPDATE_H
#define KDTREE_UPDATE_H

#include <stdint.h>
#include <stdbool.h>

#include "host/kdTree/types.h"
#include "host/kdTree/build.h"
#include "host/kdTree/search.h"

typedef enum BatchOperation
{
    BATCH_INSERT,
    BATCH_DELETE
} BatchOperation;

typedef struct DpuBatchOperation
{
    BatchOperation type;
    uint64_t targetNodeAddr;
    uint32_t pointsCount;
    uint64_t pointsAddr;
    uint64_t callbackAddr;
} DpuBatchOperation;

typedef struct RebuildInfo
{
    KDNode* node;
    uint32_t dpuId;
    bool needsRebuild;
    point** insertedPoints;
    uint32_t insertedCount;
    point** deletedPoints;
    uint32_t deletedCount;
} RebuildInfo;

typedef struct DpuResult
{
    uint64_t batchIndex;
    uint64_t leafAddr;
} DpuResult;

bool batchInsert(point** points, size_t batchSize);
bool batchDelete(point** points, size_t batchSize);

SearchBatch* leafSearchForInsert(SearchBatch* batch, KDNode*** imbalancedNodes);
SearchBatch* leafSearchForDelete(SearchBatch* batch, KDNode*** imbalancedNodes, bool** pointsFound);

bool reconstructImbalancedSubtreesForInsert(SearchBatch* insertBatch, KDNode** imbalancedNodes, size_t batchSize);
bool reconstructImbalancedSubtreesForDelete(SearchBatch* deleteBatch, KDNode** imbalancedNodes, bool* pointsFound, size_t batchSize);

void collectPointsFromSubtree(KDNode* node, point*** collector, size_t* count, size_t* capacity);
void replaceSubtree(KDNode* oldRoot, KDNode* newRoot, KDTree* tree);
void updateParentPointers(KDNode* node, KDNode* newParent);

bool offloadBatchOperationToDpus(DpuBatchOperation* operations, uint32_t count, KDNode*** results, uint32_t** resultIndices, uint32_t* resultCount, RebuildInfo** rebuildInfos, uint32_t* rebuildCount);
RebuildInfo* collectRebuildInfoFromDpus(uint32_t* rebuildCount);
bool executePartialRebuildForInsert(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree);
bool executePartialRebuildForDelete(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree);

void removeNodeFromMap(KDNode* node);
void removeSubtreeFromMap(KDNode* node);

void invalidateReplicasForNode(KDNode* node, KDGroup** groups);

bool pointExistsInLeaf(KDNode* leaf, point* p);

#endif

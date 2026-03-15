#ifndef KDTREE_UPDATE_H
#define KDTREE_UPDATE_H

#include <stdint.h>
#include <stdbool.h>
#include "kdTree/types.h"

typedef enum BatchOperation
{
    INSERT,
    DELETE
} BatchOperation;

typedef struct
{
    BatchOperation type;
    uint32_t batchId;
    uint64_t targetNodeAddr;
    uint32_t pointsCount;
    uint64_t pointsAddr;
    uint64_t callbackAddr;
} DpuBatchOperation;

typedef struct
{
    KDNode* node;
    uint32_t nodeId;
    uint64_t dpuAddr;
    uint32_t dpuId;
    uint32_t operationCount;
    bool needsRebuild;
    point** insertedPoints;
    uint32_t insertedCount;
} RebuildInfo;

bool batchInsert(point** points, size_t batchSize, size_t totalTreeSize);
bool batchDelete(point** points, size_t batchSize, size_t totalTreeSize);

SearchBatch* leafSearchForInsert(SearchBatch* batch, KDNode*** imbalancedNodes, size_t totalTreeSize);
SearchBatch* leafSearchForDelete(SearchBatch* batch, KDNode*** imbalancedNodes, bool** pointsFound, size_t totalTreeSize);

bool reconstructImbalancedSubtreesForInsert(SearchBatch* insertBatch, KDNode** imbalancedNodes, size_t batchSize);
bool reconstructImbalancedSubtreesForDelete(SearchBatch* deleteBatch, KDNode** imbalancedNodes, bool* pointsFound, size_t batchSize);

void collectPointsFromSubtree(KDNode* node, point*** collector, size_t* count, size_t* capacity);
void replaceSubtree(KDNode* oldRoot, KDNode* newRoot, KDTree* tree);
void updateParentPointers(KDNode* node, KDNode* newParent);

bool offloadBatchOperationToDpus(DpuBatchOperation* operations, uint32_t count);
RebuildInfo* collectRebuildInfoFromDpus(uint32_t* rebuildCount);
bool executePartialRebuild(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree);

#endif

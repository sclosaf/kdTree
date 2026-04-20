#ifndef DPU_KDTREE_UPDATE_H
#define DPU_KDTREE_UPDATE_H

#include "dpu/kdTree/types.h"
#include <stdint.h>
#include <stdbool.h>

typedef enum BatchOpType
{
    BATCH_INSERT,
    BATCH_DELETE
} BatchOpType;

typedef struct BatchOperation
{
    BatchOpType type;
    uint64_t targetNodeAddr;
    uint32_t pointsCount;
    uint64_t pointsAddr;
    uint64_t callbackAddr;
} BatchOperation;

typedef struct BatchResult
{
    uint64_t batchIndex;
    uint64_t leafAddr;
    uint64_t imbalancedNodeAddr;
    bool needsRebuild;
} BatchResult;

bool incrementApproximateCounter(KDNode* node);
bool decrementApproximateCounter(KDNode* node);
bool checkBalanceViolation(KDNode* node);
void propagateCounterUpdate(KDNode* node, int delta);
KDNode* findInsertionLeaf(KDNode* node, point p);
KDNode* findDeletionLeaf(KDNode* node, point p);
int processBatchOperations(BatchOperation* operations, uint32_t count, BatchResult** results, uint32_t* resultCount);

#endif

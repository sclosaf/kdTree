#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <math.h>
#include <dpu.h>
#include <dpu_types.h>

#include "kdTree/update.h"
#include "kdTree/build.h"
#include "kdTree/search.h"
#include "kdTree/counters.h"
#include "kdTree/utils.h"

#include "environment/init.h"

typedef struct DpuResult
{
    uint64_t batchIndex;
    uint64_t leafAddr;
    uint16_t pathLength;
} DpuResult;

typedef struct DpuRebuildInfo
{
    uint64_t nodeAddr;
    uint32_t operationCount;
} DpuRebuildInfo;

bool offloadBatchOperationToDpus(DpuBatchOperation* operations, uint32_t count)
{
    if(!operations || count == 0)
        return false;

    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    DpuBatchOperation** opsPerDpu = calloc(nPim, sizeof(DpuBatchOperation*));
    uint32_t* opsCountPerDpu = calloc(nPim, sizeof(uint32_t));
    uint32_t* opsCapacityPerDpu = calloc(nPim, sizeof(uint32_t));

    for(uint32_t i = 0; i < count; ++i)
    {
        uint32_t dpuId = (uint32_t)(operations[i].targetNodeAddr & 0xFF) % nPim;

        if(opsCountPerDpu[dpuId] >= opsCapacityPerDpu[dpuId])
        {
            uint32_t newCap = opsCapacityPerDpu[dpuId] ? opsCapacityPerDpu[dpuId] * 2 : 16;
            opsPerDpu[dpuId] = realloc(opsPerDpu[dpuId], newCap * sizeof(DpuBatchOperation));
            opsCapacityPerDpu[dpuId] = newCap;
        }

        opsPerDpu[dpuId][opsCountPerDpu[dpuId]++] = operations[i];
    }

    for(uint32_t d = 0; d < nPim; ++d)
    {
        if(opsCountPerDpu[d] == 0)
            continue;

        struct dpu_set_t dpu;
        uint32_t found = 0;

        DPU_FOREACH(set, dpu)
        {
            if(found++ == d)
            {
                DPU_ASSERT(dpu_copy_to(dpu, "BATCH_COUNT", 0, &opsCountPerDpu[d], sizeof(uint32_t)));

                size_t opsSize = opsCountPerDpu[d] * sizeof(DpuBatchOperation);
                DPU_ASSERT(dpu_copy_to(dpu, "BATCH_OPERATIONS", 0, opsPerDpu[d], opsSize));

                DPU_ASSERT(dpu_launch(dpu, DPU_ASYNC));
                break;
            }
        }
    }

    DPU_ASSERT(dpu_sync(set));

    for(uint32_t d = 0; d < nPim; ++d)
        free(opsPerDpu[d]);

    free(opsPerDpu);
    free(opsCountPerDpu);
    free(opsCapacityPerDpu);
    dpu_free(set);

    return true;
}

RebuildInfo* collectRebuildInfoFromDpus(uint32_t* rebuildCount)
{
    if(!rebuildCount)
        return NULL;

    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    *rebuildCount = 0;
    RebuildInfo* allRebuilds = NULL;

    for(uint32_t d = 0; d < nPim; ++d)
    {
        struct dpu_set_t dpu;
        uint32_t found = 0;

        DPU_FOREACH(set, dpu)
        {
            if(found++ == d)
            {
                uint32_t localRebuildCount = 0;
                DPU_ASSERT(dpu_copy_from(dpu, "LOCAL_REBUILD_COUNT", 0, &localRebuildCount, sizeof(uint32_t)));

                if(localRebuildCount > 0)
                {
                    size_t rebuildsSize = localRebuildCount * sizeof(RebuildInfo);
                    RebuildInfo* dpuRebuilds = malloc(rebuildsSize);

                    DPU_ASSERT(dpu_copy_from(dpu, "LOCAL_REBUILD_INFO", 0, dpuRebuilds, rebuildsSize));

                    for(uint32_t i = 0; i < localRebuildCount; ++i)
                    {
                        dpuRebuilds[i].dpuId = d;
                        dpuRebuilds[i].dpuAddr = dpuRebuilds[i].dpuAddr;

                        dpuRebuilds[i].node = resolveNodeLocation(dpuRebuilds[i].dpuAddr, d);
                    }

                    allRebuilds = realloc(allRebuilds, (*rebuildCount + localRebuildCount) * sizeof(RebuildInfo));
                    memcpy(&allRebuilds[*rebuildCount], dpuRebuilds, rebuildsSize);
                    *rebuildCount += localRebuildCount;

                    free(dpuRebuilds);
                }

                break;
            }
        }
    }

    dpu_free(set);
    return allRebuilds;
}

bool executePartialRebuildForInsert(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree)
{
    if(!rebuilds || rebuildCount == 0 || !tree)
        return false;

    KDNode** uniqueRoots = malloc(rebuildCount * sizeof(KDNode*));
    uint32_t* uniqueCounts = calloc(rebuildCount, sizeof(uint32_t));
    uint32_t uniqueCount = 0;

    for(uint32_t i = 0; i < rebuildCount; ++i)
    {
        bool found = false;
        for(uint32_t j = 0; j < uniqueCount; ++j)
        {
            if(uniqueRoots[j] == rebuilds[i].node)
            {
                ++uniqueCounts[j];
                found = true;
                break;
            }
        }

        if(!found)
        {
            uniqueRoots[uniqueCount] = rebuilds[i].node;
            uniqueCounts[uniqueCount] = 1;
            ++uniqueCount;
        }
    }

    #pragma omp parallel for
    for(uint32_t u = 0; u < uniqueCount; ++u)
    {
        KDNode* rootToRebuild = uniqueRoots[u];

        size_t oldPointsCapacity = 1024;
        size_t oldPointsCount = 0;
        point** oldPoints = malloc(oldPointsCapacity * sizeof(point*));
        collectPointsFromSubtree(rootToRebuild, &oldPoints, &oldPointsCount, &oldPointsCapacity);

        size_t totalPoints = oldPointsCount;

        for(uint32_t r = 0; r < rebuildCount; ++r)
            if(rebuilds[r].node == rootToRebuild && rebuilds[r].insertedPoints)
                totalPoints += rebuilds[r].insertedCount;

        point** allPoints = malloc(totalPoints * sizeof(point*));
        size_t index = 0;

        for(size_t i = 0; i < oldPointsCount; ++i)
            allPoints[index++] = oldPoints[i];

        for(uint32_t r = 0; r < rebuildCount; ++r)
            if(rebuilds[r].node == rootToRebuild && rebuilds[r].insertedPoints)
                for(uint32_t j = 0; j < rebuilds[r].insertedCount; ++j)
                    allPoints[index++] = rebuilds[r].insertedPoints[j];

        if(totalPoints == 0)
        {
            KDNode* emptyLeaf = createLeafNode(NULL, 0);
            if(emptyLeaf)
                replaceSubtree(rootToRebuild, emptyLeaf, tree);
        }
        else
        {
            KDTree* rebuiltSubtree = NULL;

            if(totalPoints > getConfig()->leafWrapThreshold * getConfig()->nPim)
                rebuiltSubtree = buildPIMKDTree(allPoints, totalPoints);
            else
            {
                KDNode* newRoot = buildTreeParallel(allPoints, totalPoints, 0);
                if(newRoot)
                {
                    rebuiltSubtree = malloc(sizeof(KDTree));
                    rebuiltSubtree->root = newRoot;
                    rebuiltSubtree->totalPoints = totalPoints;
                }
            }

            if(rebuiltSubtree)
            {
                replaceSubtree(rootToRebuild, rebuiltSubtree->root, tree);

                KDNode* parent = rebuiltSubtree->root->parent;
                int delta = rebuiltSubtree->totalPoints - oldPointsCount;
                while(parent)
                {
                    if(parent->type == INTERNAL)
                        parent->data.internal.approximateCounter += delta;

                    parent = parent->parent;
                }

                free(rebuiltSubtree);
            }
        }

        free(oldPoints);
        free(allPoints);
    }

    free(uniqueRoots);
    free(uniqueCounts);

    return true;
}

static bool sendInsertBatchToDpus(SearchBatch* batch, KDNode** group1Roots, PushPullContext* context)
{
    if(!batch || !group1Roots || !context)
        return false;

    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    uint32_t** queriesPerDpu = calloc(nPim, sizeof(uint32_t*));
    uint32_t* countsPerDpu = calloc(nPim, sizeof(uint32_t));

    #pragma omp parallel for
    for(size_t i = 0; i < batch->size; ++i)
    {
        if(group1Roots[i])
        {
            uint32_t dpuId = findDpuForNode(group1Roots[i]);

            #pragma omp atomic
            ++countsPerDpu[dpuId];
        }
    }

    for(size_t d = 0; d < nPim; ++d)
        if(countsPerDpu[d] > 0)
            queriesPerDpu[d] = malloc(countsPerDpu[d] * sizeof(uint32_t));

    uint32_t* currentPos = calloc(nPim, sizeof(uint32_t));
    for(size_t i = 0; i < batch->size; ++i)
        if(group1Roots[i])
        {
            uint32_t dpuId = findDpuForNode(group1Roots[i]);
            queriesPerDpu[dpuId][currentPos[dpuId]++] = i;
        }

    for(uint32_t d = 0; d < nPim; ++d)
    {
        if(countsPerDpu[d] == 0)
            continue;

        uint32_t localCount = countsPerDpu[d];

        size_t queryDataSize = localCount * getConfig()->dimensions * sizeof(float);
        float* queryData = malloc(queryDataSize);

        for(uint32_t j = 0; j < localCount; ++j)
        {
            uint32_t batchIndex = queriesPerDpu[d][j];
            memcpy(&queryData[j * getConfig()->dimensions], batch->queries[batchIndex]->coords, getConfig()->dimensions * sizeof(float));
        }

        DpuBatchOperation op = {
            .operationType = INSERT,
            .batchId = d,
            .targetNodeAddr = (uint64_t)(uintptr_t)group1Roots[queriesPerDpu[d][0]],
            .pointsCount = localCount,
            .pointsAddr = (uint64_t)(uintptr_t)queryData
        };

        struct dpu_set_t dpu;
        uint32_t found = 0;
        DPU_FOREACH(set, dpu)
        {
            if(found++ == d)
            {
                DPU_ASSERT(dpu_copy_to(dpu, "DPU_OPERATION", 0, &op, sizeof(DpuBatchOperation)));
                DPU_ASSERT(dpu_copy_to(dpu, "QUERY_BATCH", 0, queryData, queryDataSize));
                DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
                break;
            }
        }

        free(queryData);
    }

    free(queriesPerDpu);
    free(countsPerDpu);
    free(currentPos);
    dpu_free(set);

    return true;
}

static KDNode* insertToSkeleton(KDNode* node, Bucket* bucket, uint32_t* insertCounts, uint32_t totalTreeSize, bool* needsRebuild)
{
    if(!node)
        return NULL;

    if(node->type == LEAF || (node->type == INTERNAL && (node->data.internal.left == NULL || node->data.internal.right == NULL)))
    {
        uint32_t existingCount = getNodeSize(node);
        uint32_t newCount = bucket->size;

        point** allPoints = (point**)malloc((existingCount + newCount) * sizeof(point*));
        if(!allPoints)
        {
            *needsRebuild = true;
            return node;
        }

        uint32_t index = 0;
        collectPointsFromSubtree(node, allPoints, &index, existingCount + newCount);

        for(uint32_t i = 0; i < newCount; ++i)
            allPoints[index++] = bucket->bucket[i];

        KDTree* newSubtree = buildPIMKDTree(allPoints, index);
        free(allPoints);

        if(newSubtree)
        {
            freeKDTree(node);
            return newSubtree->root;
        }
        else
        {
            *needsRebuild = true;
            return node;
        }
    }

    uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
    uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

    uint32_t leftInsertCount = 0, rightInsertCount = 0;

    for(uint32_t i = 0; i < bucket->size; ++i)
        if(bucket->bucket[i]->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
            ++leftInsertCount;
        else
            ++rightInsertCount;

    Bucket leftBucket, rightBucket;

    if(leftInsertCount > 0)
    {
        leftBucket.bucket = (point**)malloc(leftInsertCount * sizeof(point*));
        leftBucket.size = leftInsertCount;

        uint32_t leftIndex = 0;
        for(uint32_t i = 0; i < bucket->size; ++i)
            if(bucket->bucket[i]->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
                leftBucket.bucket[leftIndex++] = bucket->bucket[i];
    }

    if(rightInsertCount > 0)
    {
        rightBucket.bucket = (point**)malloc(rightInsertCount * sizeof(point*));
        rightBucket.size = rightInsertCount;

        uint32_t rightIndex = 0;
        for(uint32_t i = 0; i < bucket->size; ++i)
            if(bucket->bucket[i]->coords[node->data.internal.splitDim] >= node->data.internal.splitValue)
                rightBucket.bucket[rightIndex++] = bucket->bucket[i];
    }

    uint32_t newLeftSize = leftSize + leftInsertCount;
    uint32_t newRightSize = rightSize + rightInsertCount;
    uint32_t larger = (newLeftSize > newRightSize) ? newLeftSize : newRightSize;
    uint32_t smaller = (newLeftSize > newRightSize) ? newRightSize : newLeftSize;
    float ratio = (smaller > 0) ? (float)larger / (float)smaller : 1.0f;

    if(ratio > (1.0f + getConfig()->alpha))
    {
        *needsRebuild = true;

        uint32_t totalPoints = leftSize + rightSize + bucket->size;
        point** allPoints = (point**)malloc(totalPoints * sizeof(point*));

        uint32_t index = 0;
        collectPointsFromSubtree(node, allPoints, &index, totalPoints);

        for(uint32_t i = 0; i < bucket->size; ++i)
            allPoints[index++] = bucket->bucket[i];

        KDTree* rebuilt = buildPIMKDTree(allPoints, totalPoints);
        free(allPoints);

        free(leftBucket.bucket);
        free(rightBucket.bucket);

        if(rebuilt)
        {
            freeKDTree(node);
            return rebuilt->root;
        }

        return node;
    }

    if(leftInsertCount > 0 && node->data.internal.left)
    {
        bool leftRebuild = false;
        node->data.internal.left = insertToSkeleton(node->data.internal.left, &leftBucket, insertCounts, totalTreeSize, &leftRebuild);

        if(leftRebuild)
            *needsRebuild = true;

        free(leftBucket.bucket);
    }

    if(rightInsertCount > 0 && node->data.internal.right)
    {
        bool rightRebuild = false;
        node->data.internal.right = insertToSkeleton(node->data.internal.right, &rightBucket, insertCounts, totalTreeSize, &rightRebuild);

        if(rightRebuild)
            *needsRebuild = true;
        free(rightBucket.bucket);
    }

    if(!(*needsRebuild))
    {
        uint32_t leftSizeNew = node->data.internal.left ?  getNodeSize(node->data.internal.left) : 0;
        uint32_t rightSizeNew = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

        int delta = (leftSizeNew + rightSizeNew) - (leftSize + rightSize);

        propagateCounterUpdate(node, delta, totalTreeSize + (delta > 0 ? delta : 0), false);
    }

    return node;
}

void updateParentPointers(KDNode* node, KDNode* newParent)
{
    if(!node)
        return;

    node->parent = newParent;

    if(node->type == INTERNAL)
    {
        if(node->data.internal.left)
            updateParentPointers(node->data.internal.left, node);
        if(node->data.internal.right)
            updateParentPointers(node->data.internal.right, node);
    }
}

void replaceSubtree(KDNode* oldRoot, KDNode* newRoot, KDTree* tree)
{
    if(!oldRoot || !newRoot)
        return;

    if(!oldRoot->parent)
    {
        tree->root = newRoot;
        newRoot->parent = NULL;
    }
    else
    {
        KDNode* parent = oldRoot->parent;
        bool isLeft = (parent->data.internal.left == oldRoot);

        if(isLeft)
            parent->data.internal.left = newRoot;
        else
            parent->data.internal.right = newRoot;

        newRoot->parent = parent;
    }

    updateParentPointers(newRoot, newRoot->parent);

    if(oldRoot != newRoot)
        freeKDTree(oldRoot);
}

SearchBatch* leafSearchForInsert(SearchBatch* batch, KDNode*** imbalancedNodes, size_t totalTreeSize)
{
    if(!batch || !imbalancedNodes)
        return NULL;

    *imbalancedNodes = (KDNode**)calloc(batch->size, sizeof(KDNode*));
    if(!*imbalancedNodes)
        return NULL;

    KDTree* tree = getData()->tree;
    PushPullContext* context = initPushPullContext();
    if(!context)
    {
        free(*imbalancedNodes);
        return NULL;
    }

    KDNode** group1Roots = NULL;
    searchGroup0(batch, &group1Roots);
    if(!group1Roots)
    {
        free(*imbalancedNodes);
        freePushPullContext(context);
        return NULL;
    }

    KDNode** currentNodes = (KDNode**)malloc(batch->size * sizeof(KDNode*));
    memcpy(currentNodes, group1Roots, batch->size * sizeof(KDNode*));

    DpuBatchOperation* batchOps = NULL;
    uint32_t batchOpsCount = 0;
    uint32_t batchOpsCapacity = 0;

    for(uint8_t groupId = 1; groupId < context->numGroups; ++groupId)
    {
        bool allLeaves = true;
        for(size_t i = 0; i < batch->size; ++i)
        {
            if(currentNodes[i] && currentNodes[i]->type != LEAF)
            {
                allLeaves = false;
                break;
            }
        }

        if(allLeaves)
            break;

        memset(context->groupAccessCounts, 0, context->numGroups * sizeof(uint32_t));

        #pragma omp parallel for
        for(size_t i = 0; i < batch->size; ++i)
            if(currentNodes[i] && currentNodes[i]->type == INTERNAL)
            {
                #pragma omp atomic
                ++context->groupAccessCounts[groupId];
            }

        if(shouldPull(NULL, context->groupAccessCounts[groupId], groupId, context))
        {
            #pragma omp parallel for
            for(size_t i = 0; i < batch->size; ++i)
            {
                KDNode* node = currentNodes[i];
                if(!node || node->type == LEAF)
                    continue;

                point* q = batch->queries[i];

                propagateCounterUpdate(node, 1, totalTreeSize, true);

                if(checkBalanceViolation(node))
                {
                    (*imbalancedNodes)[i] = node;
                    continue;
                }

                if(q->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
                    currentNodes[i] = node->data.internal.left;
                else
                    currentNodes[i] = node->data.internal.right;

                ++batch->results[i].pathLength;
            }
        }
        else
        {
            for(size_t i = 0; i < batch->size; ++i)
            {
                if(!currentNodes[i] || currentNodes[i]->type == LEAF)
                    continue;

                if(batchOpsCount >= batchOpsCapacity)
                {
                    batchOpsCapacity = batchOpsCapacity ? batchOpsCapacity * 2 : 64;
                    batchOps = realloc(batchOps, batchOpsCapacity * sizeof(DpuBatchOperation));
                }

                uint64_t pointsAddr = (uint64_t)(uintptr_t)batch->queries[i]->coords;

                batchOps[batchOpsCount].type = INSERT;
                batchOps[batchOpsCount].batchId = groupId;
                batchOps[batchOpsCount].targetNodeAddr = (uint64_t)(uintptr_t)currentNodes[i];
                batchOps[batchOpsCount].pointsCount = 1;
                batchOps[batchOpsCount].pointsAddr = pointsAddr;
                batchOps[batchOpsCount].callbackAddr = (uint64_t)i;
                batchOpsCount++;
            }

            if(batchOpsCount > 0)
            {
                if(!offloadBatchOperationToDpus(batchOps, batchOpsCount))
                {
                    #pragma omp parallel for
                    for(size_t i = 0; i < batch->size; ++i)
                    {
                        KDNode* node = currentNodes[i];
                        if(!node || node->type == LEAF)
                            continue;

                        point* q = batch->queries[i];
                        propagateCounterUpdate(node, 1, totalTreeSize, true);

                        if(checkBalanceViolation(node))
                        {
                           (*imbalancedNodes)[i] = node;
                            continue;
                        }

                        if(q->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
                            currentNodes[i] = node->data.internal.left;
                        else
                            currentNodes[i] = node->data.internal.right;

                        ++batch->results[i].pathLength;
                    }
                }
                else
                {
                    struct dpu_set_t set;
                    uint32_t nPim = getConfig()->nPim;
                    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

                    RebuildInfo* rebuilds = NULL;
                    uint32_t rebuildCount = 0;

                    for(uint32_t dpuId = 0; dpuId < nPim; ++dpuId)
                    {
                        struct dpu_set_t dpu;
                        uint32_t found = 0;

                        DPU_FOREACH(set, dpu)
                        {
                            if(found++ == dpuId)
                            {
                                uint32_t resultCount = 0;
                                DPU_ASSERT(dpu_copy_from(dpu, "RESULT_COUNT", 0, &resultCount, sizeof(uint32_t)));

                                if(resultCount > 0)
                                {
                                    DpuResult* results = malloc(resultCount * sizeof(DpuResult));
                                    DPU_ASSERT(dpu_copy_from(dpu, "RESULTS", 0, results, resultCount * sizeof(DpuResult)));

                                    for(uint32_t r = 0; r < resultCount; ++r)
                                    {
                                        uint64_t index = results[r].batchIndex;
                                        if(index < batch->size)
                                        {
                                            KDNode* leaf = resolveNodeLocation(results[r].leafAddr, dpuId);

                                            if(leaf)
                                            {
                                                currentNodes[index] = leaf;
                                                batch->results[index].leaf = leaf;
                                                batch->results[index].pathLength += results[r].pathLength;
                                                ++batch->results[index].dpuAccesses;
                                            }
                                        }
                                    }

                                    free(results);
                                }

                                uint32_t localRebuildCount = 0;
                                DPU_ASSERT(dpu_copy_from(dpu, "REBUILD_COUNT", 0, &localRebuildCount, sizeof(uint32_t)));

                                if(localRebuildCount > 0)
                                {
                                    DpuRebuildInfo* dpuRebuilds = malloc(localRebuildCount * sizeof(DpuRebuildInfo));
                                    DPU_ASSERT(dpu_copy_from(dpu, "REBUILD_INFO", 0, dpuRebuilds, localRebuildCount * sizeof(DpuRebuildInfo)));

                                    rebuilds = realloc(rebuilds, (rebuildCount + localRebuildCount) * sizeof(RebuildInfo));

                                    for(uint32_t r = 0; r < localRebuildCount; ++r)
                                    {
                                        KDNode* node = resolveNodeLocation(dpuRebuilds[r].nodeAddr, dpuId);
                                        if(node)
                                        {
                                            rebuilds[rebuildCount].node = node;
                                            rebuilds[rebuildCount].dpuAddr = dpuRebuilds[r].nodeAddr;
                                            rebuilds[rebuildCount].dpuId = dpuId;
                                            rebuilds[rebuildCount].operationCount = dpuRebuilds[r].operationCount;
                                            rebuilds[rebuildCount].needsRebuild = true;
                                            ++rebuildCount;

                                            for(size_t j = 0; j < batch->size; ++j)
                                                if(currentNodes[j] == node)
                                                    (*imbalancedNodes)[j] = node;
                                        }
                                    }

                                    free(dpuRebuilds);
                                }

                                break;
                            }
                        }
                    }

                    if(rebuildCount > 0 && getConfig()->immediateRebuild)
                    {
                        executePartialRebuildForInsert(rebuilds, rebuildCount, getData()->tree);
                        free(rebuilds);
                    }
                    else if(rebuildCount > 0)
                    {
                        static RebuildInfo* savedRebuilds = NULL;
                        static uint32_t savedCount = 0;

                        savedRebuilds = realloc(savedRebuilds, (savedCount + rebuildCount) * sizeof(RebuildInfo));
                        memcpy(&savedRebuilds[savedCount], rebuilds, rebuildCount * sizeof(RebuildInfo));
                        savedCount += rebuildCount;

                        free(rebuilds);
                    }

                    dpu_free(set);
                }

                batchOpsCount = 0;
            }
        }
    }

    uint32_t rebuildCount = 0;
    RebuildInfo* rebuilds = collectRebuildInfoFromDpus(&rebuildCount);
    if(rebuilds && rebuildCount > 0)
    {
        executePartialRebuildForInsert(rebuilds, rebuildCount, tree);
        free(rebuilds);
    }

    free(batchOps);
    free(group1Roots);
    free(currentNodes);
    freePushPullContext(context);

    return batch;
}

void collectPointsFromSubtree(KDNode* node, point*** collector, size_t* count, size_t* capacity)
{
    if(!node)
        return;

    if(node->type == LEAF)
    {
        for(size_t i = 0; i < node->data.leaf.pointsCount; ++i)
        {
            if(*count >= *capacity)
            {
                *capacity *= 2;
                *collector = (point**)realloc(*collector, *capacity * sizeof(point*));
            }

            (*collector)[(*count)++] = &node->data.leaf.points[i];
        }
    }
    else
    {
        collectPointsFromSubtree(node->data.internal.left, collector, count, capacity);
        collectPointsFromSubtree(node->data.internal.right, collector, count, capacity);
    }
}

bool reconstructImbalancedSubtrees(SearchBatch* insertBatch, KDNode** imbalancedNodes, size_t batchSize)
{
    if(!insertBatch || !imbalancedNodes)
        return false;

    KDTree* tree = getData()->tree;

    KDNode** uniqueImbalanced = (KDNode**)malloc(batchSize * sizeof(KDNode*));
    uint32_t* uniqueCounts = (uint32_t*)calloc(batchSize, sizeof(uint32_t));
    point*** pointsToInsert = (point***)malloc(batchSize * sizeof(point**));
    size_t* pointsToInsertCount = (size_t*)calloc(batchSize, sizeof(size_t));
    size_t uniqueCount = 0;

    if(!uniqueImbalanced || !uniqueCounts || !pointsToInsert || !pointsToInsertCount)
    {
        free(uniqueImbalanced); free(uniqueCounts); free(pointsToInsert); free(pointsToInsertCount);
        return false;
    }

    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i])
            continue;

        bool found = false;
        for(size_t j = 0; j < uniqueCount; ++j)
        {
            if(uniqueImbalanced[j] == imbalancedNodes[i])
            {
                ++uniqueCounts[j];
                found = true;
                break;
            }
        }

        if(!found)
        {
            uniqueImbalanced[uniqueCount] = imbalancedNodes[i];
            uniqueCounts[uniqueCount] = 1;
            pointsToInsert[uniqueCount] = (point**)malloc(batchSize * sizeof(point*));
            pointsToInsert[uniqueCount][0] = insertBatch->queries[i];
            pointsToInsertCount[uniqueCount] = 1;
            ++uniqueCount;
        }
        else
        {
            for(size_t j = 0; j < uniqueCount; ++j)
            {
                if(uniqueImbalanced[j] == imbalancedNodes[i])
                {
                    pointsToInsert[j][pointsToInsertCount[j]++] = insertBatch->queries[i];
                    break;
                }
            }
        }
    }

    #pragma omp parallel for
    for(size_t u = 0; u < uniqueCount; ++u)
    {
        KDNode* imbalancedRoot = uniqueImbalanced[u];

        size_t oldPointsCapacity = 1024;
        size_t oldPointsCount = 0;
        point** oldPoints = (point**)malloc(oldPointsCapacity * sizeof(point*));
        collectPointsFromSubtree(imbalancedRoot, &oldPoints, &oldPointsCount, &oldPointsCapacity);

        size_t totalPoints = oldPointsCount + pointsToInsertCount[u];
        point** allPoints = (point**)malloc(totalPoints * sizeof(point*));

        memcpy(allPoints, oldPoints, oldPointsCount * sizeof(point*));
        memcpy(allPoints + oldPointsCount, pointsToInsert[u], pointsToInsertCount[u] * sizeof(point*));

        KDTree* rebuiltSubtree = NULL;
        if(totalPoints > getConfig()->leafWrapThreshold * getConfig()->nPim)
            rebuiltSubtree = buildPIMKDTree(allPoints, totalPoints);
        else
        {
            KDNode* newRoot = buildTreeParallel(allPoints, totalPoints, 0);
            if(newRoot)
            {
                rebuiltSubtree = (KDTree*)malloc(sizeof(KDTree));
                rebuiltSubtree->root = newRoot;
                rebuiltSubtree->totalPoints = totalPoints;
            }
        }

        if(rebuiltSubtree)
        {
            replaceSubtree(imbalancedRoot, rebuiltSubtree->root, tree);
            free(rebuiltSubtree);
        }

        free(oldPoints);
        free(allPoints);
        free(pointsToInsert[u]);
    }

    free(uniqueImbalanced);
    free(uniqueCounts);
    free(pointsToInsert);
    free(pointsToInsertCount);

    return true;
}

bool batchInsert(point** points, size_t batchSize, size_t totalTreeSize)
{
    if(!points || batchSize == 0)
        return false;

    KDTree* tree = getData()->tree;

    KDNode* skeleton = tree->root;
    Bucket* buckets = sievePoints(points, batchSize, skeleton);
    if(!buckets)
        return false;

    uint32_t numBuckets = 1 << getConfig()->sketchHeight;

    SearchBatch* searchBatch = initSearchBatch(points, batchSize);
    if(!searchBatch)
    {
        free(buckets);
        return false;
    }

    KDNode** imbalancedNodes = NULL;
    searchBatch = leafSearchForInsert(searchBatch, &imbalancedNodes, totalTreeSize);
    if(!searchBatch)
    {
        freeSearchBatch(searchBatch);
        free(buckets);
        return false;
    }

    bool reconstructionNeeded = false;
    for(size_t i = 0; i < batchSize; ++i)
    {
        if(imbalancedNodes[i])
        {
            reconstructionNeeded = true;
            break;
        }
    }

    if(reconstructionNeeded)
    {
        if(!reconstructImbalancedSubtrees(searchBatch, imbalancedNodes, batchSize))
        {
            free(imbalancedNodes);
            freeSearchBatch(searchBatch);
            free(buckets);
            return false;
        }
    }

    uint32_t* insertCounts = (uint32_t*)calloc(numBuckets, sizeof(uint32_t));
    bool globalRebuild = false;

    for(uint32_t i = 0; i < numBuckets; i++)
    {
        if(buckets[i].size == 0)
            continue;

        bool localRebuild = false;
        KDNode* updated = insertToSkeleton(skeleton, &buckets[i], insertCounts, totalTreeSize, &localRebuild);

        if(localRebuild)
        {
            globalRebuild = true;
            skeleton = updated;
        }

        insertCounts[i] += buckets[i].size;
    }

    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i] && searchBatch->results[i].leaf)
        {
            KDNode* leaf = searchBatch->results[i].leaf;
            if(leaf && leaf->type == LEAF)
            {
                if(leaf->data.leaf.pointsCount + 1 > getConfig()->leafWrapThreshold)
                {
                    size_t totalPoints = leaf->data.leaf.pointsCount + 1;
                    point** allPoints = (point**)malloc(totalPoints * sizeof(point*));

                    for(size_t j = 0; j < leaf->data.leaf.pointsCount; ++j)
                        allPoints[j] = &leaf->data.leaf.points[j];

                    allPoints[leaf->data.leaf.pointsCount] = points[i];

                    KDNode* newSubtreeRoot = buildTreeParallel(allPoints, totalPoints, 0);

                    if(newSubtreeRoot)
                        replaceSubtree(leaf, newSubtreeRoot, tree);

                    free(allPoints);
                }
                else
                {
                    leaf->data.leaf.points = (point*)realloc(leaf->data.leaf.points, (leaf->data.leaf.pointsCount + 1) * sizeof(point));

                    leaf->data.leaf.points[leaf->data.leaf.pointsCount].coords = (float*)malloc(getConfig()->dimensions * sizeof(float));

                    memcpy(leaf->data.leaf.points[leaf->data.leaf.pointsCount].coords, points[i]->coords, getConfig()->dimensions * sizeof(float));

                    ++leaf->data.leaf.pointsCount;

                    if(leaf->parent)
                        propagateCounterUpdate(leaf->parent, 1, totalTreeSize + batchSize, true);
                }
            }
        }
    }

    tree->totalPoints += batchSize;
    if(globalRebuild && skeleton != tree->root)
        tree->root = skeleton;

    free(buckets);
    free(insertCounts);
    free(imbalancedNodes);
    freeSearchBatch(searchBatch);

    return true;
}

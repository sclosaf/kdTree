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

bool executePartialRebuildForDelete(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree)
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

        size_t filteredCount = 0;
        point** filteredPoints = malloc(oldPointsCount * sizeof(point*));

        for(size_t i = 0; i < oldPointsCount; ++i)
        {
            bool shouldRemove = false;

            for(uint32_t r = 0; r < rebuildCount; ++r)
            {
                if(rebuilds[r].node == rootToRebuild && rebuilds[r].deletedPoints)
                {
                    for(uint32_t j = 0; j < rebuilds[r].deletedCount; ++j)
                    {
                        bool match = true;
                        for(uint8_t d = 0; d < getConfig()->dimensions; ++d)
                        {
                            if(fabs(oldPoints[i]->coords[d] - rebuilds[r].deletedPoints[j]->coords[d]) > 1e-6)
                            {
                                match = false;
                                break;
                            }
                        }

                        if(match)
                        {
                            shouldRemove = true;
                            break;
                        }
                    }
                }

                if(shouldRemove)
                    break;
            }

            if(!shouldRemove)
                filteredPoints[filteredCount++] = oldPoints[i];
        }

        if(filteredCount == 0)
        {
            KDNode* emptyLeaf = createLeafNode(NULL, 0);
            if(emptyLeaf)
                replaceSubtree(rootToRebuild, emptyLeaf, tree);
        }
        else
        {
            KDTree* rebuiltSubtree = NULL;

            if(filteredCount > getConfig()->leafWrapThreshold * getConfig()->nPim)
                rebuiltSubtree = buildPIMKDTree(filteredPoints, filteredCount);
            else
            {
                KDNode* newRoot = buildTreeParallel(filteredPoints, filteredCount, 0);
                if(newRoot)
                {
                    rebuiltSubtree = malloc(sizeof(KDTree));
                    rebuiltSubtree->root = newRoot;
                    rebuiltSubtree->totalPoints = filteredCount;
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
        free(filteredPoints);
    }

    free(uniqueRoots);
    free(uniqueCounts);

    return true;
}

static bool sendDeleteBatchToDpus(SearchBatch* batch, KDNode** group1Roots, bool* pointsFound, PushPullContext* context)
{
    if(!batch || !group1Roots || !pointsFound || !context)
        return false;

    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    uint32_t** queriesPerDpu = calloc(nPim, sizeof(uint32_t*));
    uint32_t* countsPerDpu = calloc(nPim, sizeof(uint32_t));

    #pragma omp parallel for
    for(size_t i = 0; i < batch->size; ++i)
    {
        if(group1Roots[i] && pointsFound[i])
        {
            uint32_t dpuId = findDpuForNode(group1Roots[i]);

            #pragma omp atomic
            countsPerDpu[dpuId]++;
        }
    }

    for(uint32_t d = 0; d < nPim; ++d)
        if(countsPerDpu[d] > 0)
            queriesPerDpu[d] = malloc(countsPerDpu[d] * sizeof(uint32_t));

    uint32_t* currentPos = calloc(nPim, sizeof(uint32_t));
    for(size_t i = 0; i < batch->size; ++i)
    {
        if(group1Roots[i] && pointsFound[i])
        {
            uint32_t dpuId = findDpuForNode(group1Roots[i]);
            queriesPerDpu[dpuId][currentPos[dpuId]++] = i;
        }
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
            uint32_t batchIdx = queriesPerDpu[d][j];
            memcpy(&queryData[j * getConfig()->dimensions], batch->queries[batchIdx]->coords, getConfig()->dimensions * sizeof(float));
        }

        DpuBatchOperation op = {
            .type = DELETE,
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

bool pointExistsInLeaf(KDNode* leaf, point* p)
{
    if(!leaf || leaf->type != LEAF)
        return false;

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
    {
        bool match = true;
        for(uint8_t d = 0; d < getConfig()->dimensions; ++d)
        {
            if(fabs(leaf->data.leaf.points[i].coords[d] - p->coords[d]) > 1e-6)
            {
                match = false;
                break;
            }
        }

        if(match)
            return true;
    }

    return false;
}

static bool removePointFromLeaf(KDNode* leaf, point* p)
{
    if(!leaf || leaf->type != LEAF || leaf->data.leaf.pointsCount == 0)
        return false;

    int removeIndex = -1;

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
    {
        bool match = true;

        for(uint8_t d = 0; d < getConfig()->dimensions; ++d)
        {
            if(fabs(leaf->data.leaf.points[i].coords[d] - p->coords[d]) > 1e-6)
            {
                match = false;
                break;
            }
        }

        if(match)
        {
            removeIndex = i;
            break;
        }
    }

    if(removeIndex == -1)
        return false;

    free(leaf->data.leaf.points[removeIndex].coords);

    if(removeIndex < leaf->data.leaf.pointsCount - 1)
        leaf->data.leaf.points[removeIndex] = leaf->data.leaf.points[leaf->data.leaf.pointsCount - 1];

    leaf->data.leaf.pointsCount--;

    if(leaf->data.leaf.pointsCount > 0)
    {
        point* newPoints = (point*)realloc(leaf->data.leaf.points, leaf->data.leaf.pointsCount * sizeof(point));
        if(newPoints)
            leaf->data.leaf.points = newPoints;
    }
    else
    {
        free(leaf->data.leaf.points);
        leaf->data.leaf.points = NULL;
    }

    return true;
}

SearchBatch* leafSearchForDelete(SearchBatch* batch, KDNode*** imbalancedNodes, bool** pointsFound, size_t totalTreeSize)
{
    if(!batch || !imbalancedNodes || !pointsFound)
        return NULL;

    *imbalancedNodes = (KDNode**)calloc(batch->size, sizeof(KDNode*));
    *pointsFound = (bool*)calloc(batch->size, sizeof(bool));

    if(!*imbalancedNodes || !*pointsFound)
    {
        free(*imbalancedNodes);
        free(*pointsFound);
        return NULL;
    }

    KDTree* tree = getData()->tree;
    PushPullContext* context = initPushPullContext();
    if(!context)
    {
        free(*imbalancedNodes);
        free(*pointsFound);
        return NULL;
    }

    KDNode** group1Roots = NULL;
    searchGroup0(batch, &group1Roots);
    if(!group1Roots)
    {
        free(*imbalancedNodes);
        free(*pointsFound);
        freePushPullContext(context);
        return NULL;
    }

    KDNode** currentNodes = (KDNode**)malloc(batch->size * sizeof(KDNode*));
    memcpy(currentNodes, group1Roots, batch->size * sizeof(KDNode*));

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

        #pragma omp parallel for
        for(size_t i = 0; i < batch->size; ++i)
        {
            KDNode* node = currentNodes[i];
            if(!node || node->type == LEAF)
                continue;

            point* q = batch->queries[i];

            if(q->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
                currentNodes[i] = node->data.internal.left;
            else
                currentNodes[i] = node->data.internal.right;
        }
    }

    for(size_t i = 0; i < batch->size; ++i)
        if(currentNodes[i] && currentNodes[i]->type == LEAF)
        {
            (*pointsFound)[i] = pointExistsInLeaf(currentNodes[i], batch->queries[i]);
            batch->results[i].leaf = currentNodes[i];
        }

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
        for(size_t i = 0; i < batch->size; ++i)
            if((*pointsFound)[i] && currentNodes[i] && currentNodes[i]->type == INTERNAL)
                context->groupAccessCounts[groupId]++;

        if(shouldPull(NULL, context->groupAccessCounts[groupId], groupId, context))
        {
            #pragma omp parallel for
            for(size_t i = 0; i < batch->size; ++i)
            {
                if(!(*pointsFound)[i])
                    continue;

                KDNode* node = currentNodes[i];
                if(!node || node->type == LEAF)
                    continue;

                point* q = batch->queries[i];

                propagateCounterUpdate(node, -1, totalTreeSize, true);

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
                if(!(*pointsFound)[i] || !currentNodes[i] || currentNodes[i]->type == LEAF)
                    continue;

                if(batchOpsCount >= batchOpsCapacity)
                {
                    batchOpsCapacity = batchOpsCapacity ? batchOpsCapacity * 2 : 64;
                    batchOps = realloc(batchOps, batchOpsCapacity * sizeof(DpuBatchOperation));
                }

                uint64_t pointsAddr = (uint64_t)(uintptr_t)batch->queries[i]->coords;

                batchOps[batchOpsCount].type = DELETE;
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
                        if(!(*pointsFound)[i])
                            continue;

                        KDNode* node = currentNodes[i];
                        if(!node || node->type == LEAF)
                            continue;

                        point* q = batch->queries[i];
                        propagateCounterUpdate(node, -1, totalTreeSize, true);

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
                                        uint64_t idx = results[r].batchIndex;
                                        if(idx < batch->size)
                                        {
                                            KDNode* leaf = resolveNodeLocation(results[r].leafAddr, dpuId);

                                            if(leaf)
                                            {
                                                currentNodes[idx] = leaf;
                                                batch->results[idx].leaf = leaf;
                                                batch->results[idx].pathLength += results[r].pathLength;
                                                ++batch->results[idx].dpuAccesses;
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
                                            rebuilds[rebuildCount].deletedPoints = NULL;
                                            rebuilds[rebuildCount].deletedCount = 0;
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
                        executePartialRebuildForDelete(rebuilds, rebuildCount, getData()->tree);
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
        executePartialRebuildForDelete(rebuilds, rebuildCount, tree);
        free(rebuilds);
    }

    free(batchOps);
    free(group1Roots);
    free(currentNodes);
    freePushPullContext(context);

    return batch;
}

bool reconstructImbalancedSubtreesForDelete(SearchBatch* deleteBatch, KDNode** imbalancedNodes, bool* pointsFound, size_t batchSize)
{
    if(!deleteBatch || !imbalancedNodes || !pointsFound) return false;

    KDTree* tree = getData()->tree;

    KDNode** uniqueImbalanced = (KDNode**)malloc(batchSize * sizeof(KDNode*));
    uint32_t* uniqueCounts = (uint32_t*)calloc(batchSize, sizeof(uint32_t));
    point*** pointsToRemove = (point***)malloc(batchSize * sizeof(point**));
    size_t* pointsToRemoveCount = (size_t*)calloc(batchSize, sizeof(size_t));
    size_t uniqueCount = 0;

    if(!uniqueImbalanced || !uniqueCounts || !pointsToRemove || !pointsToRemoveCount)
    {
        free(uniqueImbalanced); free(uniqueCounts); free(pointsToRemove); free(pointsToRemoveCount);
        return false;
    }

    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i] || !pointsFound[i])
            continue;

        bool found = false;
        for(size_t j = 0; j < uniqueCount; ++j)
        {
            if(uniqueImbalanced[j] == imbalancedNodes[i])
            {
                uniqueCounts[j]++;
                found = true;
                break;
            }
        }

        if(!found)
        {
            uniqueImbalanced[uniqueCount] = imbalancedNodes[i];
            uniqueCounts[uniqueCount] = 1;
            pointsToRemove[uniqueCount] = (point**)malloc(batchSize * sizeof(point*));
            pointsToRemove[uniqueCount][0] = deleteBatch->queries[i];
            pointsToRemoveCount[uniqueCount] = 1;
            uniqueCount++;
        }
        else
        {
            for(size_t j = 0; j < uniqueCount; ++j)
            {
                if(uniqueImbalanced[j] == imbalancedNodes[i])
                {
                    pointsToRemove[j][pointsToRemoveCount[j]++] = deleteBatch->queries[i];
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

        size_t filteredCount = 0;
        point** filteredPoints = (point**)malloc(oldPointsCount * sizeof(point*));

        for(size_t i = 0; i < oldPointsCount; ++i)
        {
            bool shouldRemove = false;
            for(size_t j = 0; j < pointsToRemoveCount[u] && !shouldRemove; ++j)
            {
                bool match = true;
                for(uint8_t d = 0; d < getConfig()->dimensions; ++d)
                {
                    if(fabs(oldPoints[i]->coords[d] - pointsToRemove[u][j]->coords[d]) > 1e-6)
                    {
                        match = false;
                        break;
                    }
                }

                if(match)
                    shouldRemove = true;
            }

            if(!shouldRemove)
                filteredPoints[filteredCount++] = oldPoints[i];
        }

        if(filteredCount == 0)
        {
            KDNode* emptyLeaf = createLeafNode(NULL, 0);
            if(emptyLeaf)
                replaceSubtree(imbalancedRoot, emptyLeaf, tree);
        }
        else
        {
            KDTree* rebuiltSubtree = buildPIMKDTree(filteredPoints, filteredCount);
            if(rebuiltSubtree)
            {
                replaceSubtree(imbalancedRoot, rebuiltSubtree->root, tree);
                free(rebuiltSubtree);
            }
        }

        free(oldPoints);
        free(filteredPoints);
        free(pointsToRemove[u]);
    }

    free(uniqueImbalanced);
    free(uniqueCounts);
    free(pointsToRemove);
    free(pointsToRemoveCount);

    return true;
}

bool batchDelete(point** points, size_t batchSize, size_t totalTreeSize)
{
    if(!points || batchSize == 0)
        return false;

    KDTree* tree = getData()->tree;
    if(!tree)
        return false;

    KDNode* skeleton = tree->root;

    Bucket* buckets = sievePoints(points, batchSize, skeleton);
    if(!buckets) return false;

    uint32_t numBuckets = 1 << getConfig()->sketchHeight;

    SearchBatch* searchBatch = initSearchBatch(points, batchSize);
    if(!searchBatch)
    {
        free(buckets);
        return false;
    }

    KDNode** imbalancedNodes = NULL;
    bool* pointsFound = NULL;
    searchBatch = leafSearchForDelete(searchBatch, &imbalancedNodes, &pointsFound, totalTreeSize);

    if(!searchBatch || !imbalancedNodes || !pointsFound)
    {
        freeSearchBatch(searchBatch);
        free(buckets);
        free(imbalancedNodes);
        free(pointsFound);
        return false;
    }

    bool reconstructionNeeded = false;
    for(size_t i = 0; i < batchSize && !reconstructionNeeded; ++i)
        if(imbalancedNodes[i] && pointsFound[i])
            reconstructionNeeded = true;

    if(reconstructionNeeded)
    {
        if(!reconstructImbalancedSubtreesForDelete(searchBatch, imbalancedNodes, pointsFound, batchSize))
        {
            free(imbalancedNodes);
            free(pointsFound);
            freeSearchBatch(searchBatch);
            free(buckets);
            return false;
        }
    }

    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i] && pointsFound[i] && searchBatch->results[i].leaf)
        {
            KDNode* leaf = searchBatch->results[i].leaf;
            if(leaf && leaf->type == LEAF &&  removePointFromLeaf(leaf, points[i]) && leaf->parent)
                propagateCounterUpdate(leaf->parent, -1, tree->totalPoints - 1, true);
        }
    }

    size_t removedCount = 0;
    for(size_t i = 0; i < batchSize; ++i)
        if(pointsFound[i])
            ++removedCount;

    tree->totalPoints -= removedCount;

    free(buckets);
    free(imbalancedNodes);
    free(pointsFound);
    freeSearchBatch(searchBatch);

    return true;
}

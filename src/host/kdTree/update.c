#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>
#include <dpu.h>
#include <dpu_types.h>

#include "host/kdTree/update.h"
#include "host/kdTree/build.h"
#include "host/kdTree/search.h"
#include "host/kdTree/counters.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/free.h"

#include "host/environment/init.h"
#include "host/management/logging.h"

#include "host/loader/dispatcher.h"

static KDNode* insertToSkeleton(KDNode* node, Bucket* bucket, uint32_t* insertCounts, bool* needsRebuild)
{
    logMessage("Inserting bucket into skeleton", DEBUG);

    if(!node)
    {
        logMessage("Null node provided to insertToSkeleton", ERROR);
        return NULL;
    }

    if(node->type == LEAF || (node->type == INTERNAL && (node->data.internal.left == NULL || node->data.internal.right == NULL)))
    {
        uint32_t existingCount = getNodeSize(node);
        uint32_t newCount = bucket->size;

        char msg[128];
        snprintf(msg, sizeof(msg), "Merging bucket into leaf: existing=%u, new=%u, total=%u", existingCount, newCount, existingCount + newCount);
        logMessage(msg, INFO);

        point** allPoints = (point**)malloc((existingCount + newCount) * sizeof(point*));
        if(!allPoints)
        {
            logMessage("Failed to allocate points array for skeleton insert", ERROR);
            *needsRebuild = true;
            return node;
        }

        size_t count = 0;
        size_t capacity = existingCount + newCount;
        collectPointsFromSubtree(node, &allPoints, &count, &capacity);
        uint32_t index = count;

        for(uint32_t i = 0; i < newCount; ++i)
            allPoints[index++] = bucket->bucket[i];

        KDTree* newSubtree = buildPIMkdtree(allPoints, index);
        free(allPoints);

        if(newSubtree)
        {
            freeKDTree(node);
            logMessage("Leaf node rebuilt with new points", INFO);
            return newSubtree->root;
        }
        else
        {
            logMessage("Failed to rebuild subtree at leaf, flagging rebuild", ERROR);
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

    char msg[128];
    snprintf(msg, sizeof(msg), "Bucket split: left=%u, right=%u (subtree sizes: left=%u, right=%u)", leftInsertCount, rightInsertCount, leftSize, rightSize);
    logMessage(msg, INFO);

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

    snprintf(msg, sizeof(msg), "Balance check: newLeft=%u, newRight=%u, ratio=%.3f, threshold=%.3f", newLeftSize, newRightSize, ratio, 1.0f + getAlpha());
    logMessage(msg, INFO);

    if(ratio > (1.0f + getAlpha()))
    {
        logMessage("Balance violation detected, rebuilding subtree", DEBUG);

        *needsRebuild = true;

        uint32_t totalPoints = leftSize + rightSize + bucket->size;

        snprintf(msg, sizeof(msg), "Rebuilding unbalanced subtree: totalPoints=%u", totalPoints);
        logMessage(msg, INFO);

        point** allPoints = (point**)malloc(totalPoints * sizeof(point*));

        size_t count = 0;
        size_t capacity = totalPoints;
        collectPointsFromSubtree(node, &allPoints, &count, &capacity);
        uint32_t index = count;

        for(uint32_t i = 0; i < bucket->size; ++i)
            allPoints[index++] = bucket->bucket[i];

        KDTree* rebuilt = buildPIMkdtree(allPoints, totalPoints);
        free(allPoints);

        free(leftBucket.bucket);
        free(rightBucket.bucket);

        if(rebuilt)
        {
            freeKDTree(node);
            logMessage("Subtree rebuilt after balance violation", INFO);
            return rebuilt->root;
        }

        return node;
    }

    if(leftInsertCount > 0 && node->data.internal.left)
    {
        bool leftRebuild = false;
        node->data.internal.left = insertToSkeleton(node->data.internal.left, &leftBucket, insertCounts, &leftRebuild);

        if(leftRebuild)
            *needsRebuild = true;

        free(leftBucket.bucket);
    }

    if(rightInsertCount > 0 && node->data.internal.right)
    {
        bool rightRebuild = false;
        node->data.internal.right = insertToSkeleton(node->data.internal.right, &rightBucket, insertCounts, &rightRebuild);

        if(rightRebuild)
            *needsRebuild = true;

        free(rightBucket.bucket);
    }

    if(!(*needsRebuild))
    {
        uint32_t leftSizeNew = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
        uint32_t rightSizeNew = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

        int delta = (leftSizeNew + rightSizeNew) - (leftSize + rightSize);

        snprintf(msg, sizeof(msg), "Counter update: delta=%d (left: %u->%u, right: %u->%u)", delta, leftSize, leftSizeNew, rightSize, rightSizeNew);
        logMessage(msg, INFO);

        propagateCounterUpdate(node, delta, false);
    }

    return node;
}

static float getTolerance()
{
    double minCoord = getMinCoord();
    double maxCoord = getMaxCoord();
    double coordRange = maxCoord - minCoord;

    double tolerance = coordRange * 1e-6;

    if(tolerance < 1e-9)
        tolerance = 1e-6;

    return (float)tolerance;
}

static bool removePointFromLeaf(KDNode* leaf, point* p)
{
    logMessage("Removing point from leaf", DEBUG);

    if(!leaf || leaf->type != LEAF || leaf->data.leaf.pointsCount == 0)
    {
        logMessage("Invalid leaf or empty leaf provided", ERROR);
        return false;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Searching point in leaf: pointsCount=%zu", leaf->data.leaf.pointsCount);
    logMessage(msg, INFO);

    float tolerance = getTolerance();
    int removeIndex = -1;

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
    {
        bool match = true;

        for(uint8_t d = 0; d < getDimensions(); ++d)
        {
            if(fabs(leaf->data.leaf.points[i].coords[d] - p->coords[d]) > tolerance)
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
    {
        logMessage("Point not found in leaf", DEBUG);
        return false;
    }

    free(leaf->data.leaf.points[removeIndex].coords);

    if(removeIndex < leaf->data.leaf.pointsCount - 1)
        leaf->data.leaf.points[removeIndex] = leaf->data.leaf.points[leaf->data.leaf.pointsCount - 1];

    --leaf->data.leaf.pointsCount;

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

    snprintf(msg, sizeof(msg), "Point removed from leaf: remaining=%zu", leaf->data.leaf.pointsCount);
    logMessage(msg, INFO);
    return true;
}

bool executePartialRebuildForInsert(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree)
{
    logMessage("Executing partial rebuild for insert", DEBUG);

    if(!rebuilds || rebuildCount == 0 || !tree)
    {
        logMessage("Invalid arguments for partial rebuild", ERROR);
        return false;
    }

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

    char msg[128];
    snprintf(msg, sizeof(msg), "Unique subtrees to rebuild: %u (total rebuild infos: %u)", uniqueCount, rebuildCount);
    logMessage(msg, INFO);

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

        char localMsg[128];
        snprintf(localMsg, sizeof(localMsg), "Rebuilding subtree[%u]: existing=%zu, total after insert=%zu", u, oldPointsCount, totalPoints);
        logMessage(localMsg, INFO);

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

            if(totalPoints > getLeafWrapThreshold() * getNPim())
                rebuiltSubtree = buildPIMkdtree(allPoints, totalPoints);
            else
            {
                KDNode* newRoot = buildTree(allPoints, totalPoints, 0);
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

                snprintf(localMsg, sizeof(localMsg), "Subtree[%u] rebuilt: delta=%d, newSize=%zu", u, delta, totalPoints);
                logMessage(localMsg, INFO);

                while(parent)
                {
                    if(parent->type == INTERNAL)
                        parent->data.internal.approximateCounter += delta;

                    parent = parent->parent;
                }

                free(rebuiltSubtree);
            }
            else
                logMessage("Failed to rebuild subtree for insert", ERROR);
        }

        free(oldPoints);
        free(allPoints);
    }

    free(uniqueRoots);
    free(uniqueCounts);

    logMessage("Partial rebuild for insert completed", INFO);
    return true;
}

void removeNodeFromMap(KDNode* node)
{
    logMessage("Removing node from location map", DEBUG);

    Data* data = getData();
    if(!data || !data->map)
    {
        logMessage("Data or map is null, nothing to remove", DEBUG);
        return;
    }

    NodeLocationMap* map = data->map;
    for(size_t i = 0; i < map->count; ++i)
    {
        if(map->nodes[i] == node)
        {
            map->nodes[i] = map->nodes[map->count - 1];
            map->dpuIds[i] = map->dpuIds[map->count - 1];
            map->dpuAddresses[i] = map->dpuAddresses[map->count - 1];
            map->count--;
            logMessage("Node removed from location map", INFO);
            break;
        }
    }
}

void removeSubtreeFromMap(KDNode* node)
{
    if(!node)
        return;

    removeNodeFromMap(node);

    if(node->type == INTERNAL)
    {
        removeSubtreeFromMap(node->data.internal.left);
        removeSubtreeFromMap(node->data.internal.right);
    }
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
    logMessage("Replacing subtree", DEBUG);

    if(!oldRoot || !newRoot)
    {
        logMessage("Null old or new root provided to replaceSubtree", ERROR);
        return;
    }

    if(tree->groups)
    {
        size_t oldSize = getNodeSize(oldRoot);
        int groupId = findGroupForSize(oldSize, tree->groups);

        if(groupId >= 0 && tree->groups[groupId])
        {
            for(size_t i = 0; i < tree->groups[groupId]->replicaCount; ++i)
            {
                if(isNodeInSubtree(tree->groups[groupId]->replicas[i]->masterNode, oldRoot))
                {
                    freeReplica(tree->groups[groupId]->replicas[i]);
                    tree->groups[groupId]->replicas[i] = NULL;
                }
            }
        }
    }

    removeSubtreeFromMap(oldRoot);

    if(!oldRoot->parent)
    {
        tree->root = newRoot;
        newRoot->parent = NULL;
        logMessage("Replaced tree root", INFO);
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
        logMessage("Subtree replaced in parent", INFO);
    }

    updateParentPointers(newRoot, newRoot->parent);

    if(oldRoot != newRoot)
        freeKDTree(oldRoot);

    if(tree->groups && newRoot)
    {
        size_t newSize = getNodeSize(newRoot);
        int newGroupId = findGroupForSize(newSize, tree->groups);

        if(newGroupId > 0 && tree->groups[newGroupId])
            buildGroupReplicas(tree->groups[newGroupId]);
    }
}

SearchBatch* leafSearchForInsert(SearchBatch* batch, KDNode*** imbalancedNodes)
{
    logMessage("Starting leaf search for insert", DEBUG);

    if(!batch || !imbalancedNodes)
    {
        logMessage("Invalid arguments for leaf search for insert", ERROR);
        return NULL;
    }

    *imbalancedNodes = (KDNode**)calloc(batch->size, sizeof(KDNode*));
    if(!*imbalancedNodes)
    {
        logMessage("Failed to allocate imbalanced nodes array", ERROR);
        return NULL;
    }

    PushPullContext* context = initPushPullContext();
    if(!context)
    {
        logMessage("Failed to initialize push-pull context", ERROR);
        free(*imbalancedNodes);
        return NULL;
    }

    KDNode** group1Roots = searchGroup0(batch);
    if(!group1Roots)
    {
        logMessage("Group 0 search returned null roots", ERROR);
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
        {
            logMessage("All nodes reached leaves, stopping traversal", INFO);
            break;
        }

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
            logMessage("Pulling nodes to CPU for insert traversal", DEBUG);

            #pragma omp parallel for
            for(size_t i = 0; i < batch->size; ++i)
            {
                KDNode* node = currentNodes[i];
                if(!node || node->type == LEAF)
                    continue;

                point* q = batch->queries[i];

                propagateCounterUpdate(node, 1, true);

                if(checkBalanceViolation(node))
                {
                    (*imbalancedNodes)[i] = node;
                    continue;
                }

                if(q->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
                    currentNodes[i] = node->data.internal.left;
                else
                    currentNodes[i] = node->data.internal.right;
            }
        }
        else
        {
            logMessage("Pushing insert operations to DPUs", DEBUG);

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

                batchOps[batchOpsCount].type = BATCH_INSERT;
                batchOps[batchOpsCount].targetNodeAddr = (uint64_t)(uintptr_t)currentNodes[i];
                batchOps[batchOpsCount].pointsCount = 1;
                batchOps[batchOpsCount].pointsAddr = pointsAddr;
                batchOps[batchOpsCount].callbackAddr = (uint64_t)i;
                batchOpsCount++;
            }

            if(batchOpsCount > 0)
            {
                KDNode** results = NULL;
                uint32_t* resultIndices = NULL;
                uint32_t resultCount = 0;
                RebuildInfo* rebuilds = NULL;
                uint32_t rebuildCount = 0;

                if(offloadBatchOperationToDpus(batchOps, batchOpsCount, &results, &resultIndices, &resultCount, &rebuilds, &rebuildCount))
                {
                    for(uint32_t r = 0; r < resultCount; ++r)
                    {
                        uint32_t idx = resultIndices[r];
                        if(idx < batch->size && results[r])
                        {
                            currentNodes[idx] = results[r];
                            batch->results[idx] = results[r];
                        }
                    }

                    for(uint32_t r = 0; r < rebuildCount; ++r)
                    {
                        KDNode* node = rebuilds[r].node;
                        if(node)
                            for(size_t j = 0; j < batch->size; ++j)
                                if(currentNodes[j] == node)
                                    (*imbalancedNodes)[j] = node;
                    }

                    if(rebuildCount > 0)
                    {
                        logMessage("Rebuild required after DPU insert, executing partial rebuild", DEBUG);
                        executePartialRebuildForInsert(rebuilds, rebuildCount, getData()->tree);
                    }

                    free(results);
                    free(resultIndices);
                    free(rebuilds);
                }
                else
                {
                    logMessage("DPU batch operation failed during insert", ERROR);
                    return NULL;
                }

                batchOpsCount = 0;
            }
        }
    }

    free(batchOps);
    free(group1Roots);
    free(currentNodes);
    freePushPullContext(context);

    logMessage("Leaf search for insert completed", INFO);
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

bool reconstructImbalancedSubtreesForInsert(SearchBatch* insertBatch, KDNode** imbalancedNodes, size_t batchSize)
{
    logMessage("Reconstructing imbalanced subtrees for insert", DEBUG);

    if(!insertBatch || !imbalancedNodes)
    {
        logMessage("Invalid arguments for subtree reconstruction", ERROR);
        return false;
    }

    KDTree* tree = getData()->tree;

    KDNode** uniqueImbalanced = (KDNode**)malloc(batchSize * sizeof(KDNode*));
    uint32_t* uniqueCounts = (uint32_t*)calloc(batchSize, sizeof(uint32_t));
    point*** pointsToInsert = (point***)malloc(batchSize * sizeof(point**));
    size_t* pointsToInsertCount = (size_t*)calloc(batchSize, sizeof(size_t));
    size_t uniqueCount = 0;

    if(!uniqueImbalanced || !uniqueCounts || !pointsToInsert || !pointsToInsertCount)
    {
        logMessage("Failed to allocate reconstruction arrays", ERROR);
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

    char msg[128];
    snprintf(msg, sizeof(msg), "Unique imbalanced subtrees to reconstruct: %zu (batchSize=%zu)", uniqueCount, batchSize);
    logMessage(msg, INFO);

    #pragma omp parallel for
    for(size_t u = 0; u < uniqueCount; ++u)
    {
        KDNode* imbalancedRoot = uniqueImbalanced[u];

        size_t oldPointsCapacity = 1024;
        size_t oldPointsCount = 0;
        point** oldPoints = (point**)malloc(oldPointsCapacity * sizeof(point*));
        collectPointsFromSubtree(imbalancedRoot, &oldPoints, &oldPointsCount, &oldPointsCapacity);

        size_t totalPoints = oldPointsCount + pointsToInsertCount[u];

        char localMsg[128];
        snprintf(localMsg, sizeof(localMsg), "Imbalanced subtree[%zu]: existing=%zu, toInsert=%zu, total=%zu", u, oldPointsCount, pointsToInsertCount[u], totalPoints);
        logMessage(localMsg, INFO);

        point** allPoints = (point**)malloc(totalPoints * sizeof(point*));

        memcpy(allPoints, oldPoints, oldPointsCount * sizeof(point*));
        memcpy(allPoints + oldPointsCount, pointsToInsert[u], pointsToInsertCount[u] * sizeof(point*));

        KDTree* rebuiltSubtree = NULL;
        if(totalPoints > getLeafWrapThreshold() * getNPim())
            rebuiltSubtree = buildPIMkdtree(allPoints, totalPoints);
        else
        {
            KDNode* newRoot = buildTree(allPoints, totalPoints, 0);
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
            logMessage("Imbalanced subtree reconstructed and replaced", INFO);
        }
        else
            logMessage("Failed to rebuild imbalanced subtree for insert", ERROR);

        free(oldPoints);
        free(allPoints);
        free(pointsToInsert[u]);
    }

    free(uniqueImbalanced);
    free(uniqueCounts);
    free(pointsToInsert);
    free(pointsToInsertCount);

    logMessage("Imbalanced subtree reconstruction for insert completed", INFO);
    return true;
}

bool batchInsert(point** points, size_t batchSize)
{
    logMessage("Starting batch insert", DEBUG);

    if(!points || batchSize == 0)
    {
        logMessage("Invalid arguments for batch insert", ERROR);
        return false;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Batch insert: batchSize=%zu", batchSize);
    logMessage(msg, INFO);

    KDTree* tree = getData()->tree;
    uint32_t numBuckets = getChunkSize();

    KDNode* skeleton = tree->root;
    Bucket* buckets = sievePoints(points, batchSize, skeleton);
    if(!buckets)
    {
        logMessage("Failed to sieve points into buckets", ERROR);
        return false;
    }

    SearchBatch* searchBatch = initSearchBatch(points, batchSize);
    if(!searchBatch)
    {
        logMessage("Failed to initialize search batch for insert", ERROR);
        free(buckets);
        return false;
    }

    KDNode** imbalancedNodes = NULL;
    searchBatch = leafSearchForInsert(searchBatch, &imbalancedNodes);
    if(!searchBatch)
    {
        logMessage("Leaf search for insert failed", ERROR);
        freeSearchBatch(searchBatch);
        free(buckets);
        return false;
    }

    uint32_t imbalancedCount = 0;
    for(size_t i = 0; i < batchSize; ++i)
        if(imbalancedNodes[i])
            ++imbalancedCount;

    bool reconstructionNeeded = imbalancedCount > 0;

    if(reconstructionNeeded)
    {
        snprintf(msg, sizeof(msg), "Imbalanced nodes detected: %u, starting reconstruction", imbalancedCount);
        logMessage(msg, DEBUG);

        if(!reconstructImbalancedSubtreesForInsert(searchBatch, imbalancedNodes, batchSize))
        {
            logMessage("Reconstruction of imbalanced subtrees failed", ERROR);
            free(imbalancedNodes);
            freeSearchBatch(searchBatch);
            free(buckets);
            return false;
        }
    }

    uint32_t* insertCounts = (uint32_t*)calloc(numBuckets, sizeof(uint32_t));
    bool globalRebuild = false;
    uint32_t rebuiltBuckets = 0;

    for(uint32_t i = 0; i < numBuckets; ++i)
    {
        if(buckets[i].size == 0)
            continue;

        bool localRebuild = false;
        KDNode* updated = insertToSkeleton(skeleton, &buckets[i], insertCounts, &localRebuild);

        if(localRebuild)
        {
            globalRebuild = true;
            skeleton = updated;
            ++rebuiltBuckets;
        }

        insertCounts[i] += buckets[i].size;
    }

    if(rebuiltBuckets > 0)
    {
        snprintf(msg, sizeof(msg), "Skeleton insert completed: rebuiltBuckets=%u/%u", rebuiltBuckets, numBuckets);
        logMessage(msg, INFO);
    }

    uint32_t leafExpanded = 0;
    uint32_t leafRebuilt = 0;

    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i] && searchBatch->results[i])
        {
            KDNode* leaf = searchBatch->results[i];
            if(leaf && leaf->type == LEAF)
            {
                if(leaf->data.leaf.pointsCount + 1 > getLeafWrapThreshold())
                {
                    logMessage("Leaf threshold exceeded, rebuilding leaf as subtree", DEBUG);

                    size_t totalPoints = leaf->data.leaf.pointsCount + 1;
                    point** allPoints = (point**)malloc(totalPoints * sizeof(point*));

                    for(size_t j = 0; j < leaf->data.leaf.pointsCount; ++j)
                        allPoints[j] = &leaf->data.leaf.points[j];

                    allPoints[leaf->data.leaf.pointsCount] = points[i];

                    KDNode* newSubtreeRoot = buildTree(allPoints, totalPoints, 0);

                    if(newSubtreeRoot)
                        replaceSubtree(leaf, newSubtreeRoot, tree);

                    free(allPoints);
                    ++leafRebuilt;
                }
                else
                {
                    leaf->data.leaf.points = (point*)realloc(leaf->data.leaf.points, (leaf->data.leaf.pointsCount + 1) * sizeof(point));
                    leaf->data.leaf.points[leaf->data.leaf.pointsCount].coords = (float*)malloc(getDimensions() * sizeof(float));

                    memcpy(leaf->data.leaf.points[leaf->data.leaf.pointsCount].coords, points[i]->coords, getDimensions() * sizeof(float));

                    ++leaf->data.leaf.pointsCount;

                    if(leaf->parent)
                        propagateCounterUpdate(leaf->parent, 1 + batchSize, true);

                    ++leafExpanded;
                }
            }
        }
    }

    snprintf(msg, sizeof(msg), "Leaf updates: expanded=%u, rebuilt=%u", leafExpanded, leafRebuilt);
    logMessage(msg, INFO);

    tree->totalPoints += batchSize;

    snprintf(msg, sizeof(msg), "Tree total points updated: totalPoints=%u", tree->totalPoints);
    logMessage(msg, INFO);

    if(globalRebuild && skeleton != tree->root)
        tree->root = skeleton;

    if(tree->groups)
    {
        for(size_t i = 0; i < batchSize; ++i)
        {
            if(imbalancedNodes[i])
                invalidateReplicasForNode(imbalancedNodes[i], tree->groups);
            else if(searchBatch->results[i])
                invalidateReplicasForNode(searchBatch->results[i], tree->groups);
        }
    }

    free(buckets);
    free(insertCounts);
    free(imbalancedNodes);
    freeSearchBatch(searchBatch);

    logMessage("Batch insert completed", INFO);
    return true;
}

bool offloadBatchOperationToDpus(DpuBatchOperation* operations, uint32_t count, KDNode*** results, uint32_t** resultIndices, uint32_t* resultCount, RebuildInfo** rebuildInfos, uint32_t* rebuildCount)
{
    logMessage("Offloading batch operation to DPUs", DEBUG);

    if(!operations || count == 0)
    {
        logMessage("Invalid arguments for DPU batch offload", ERROR);
        return false;
    }

    uint32_t nPim = getNPim();
    DpuResult** resultsPerDpu = NULL;
    uint32_t* resultCountsPerDpu = NULL;

    int ret = dispatchBatchUpdate(operations, count, &resultsPerDpu, &resultCountsPerDpu, nPim);

    if(ret != 0)
    {
        logMessage("Batch update dispatch to DPUs failed", ERROR);
        return false;
    }

    *resultCount = 0;
    *results = NULL;
    *resultIndices = NULL;
    *rebuildInfos = NULL;
    *rebuildCount = 0;

    for(uint32_t dpuId = 0; dpuId < nPim; ++dpuId)
    {
        if(resultCountsPerDpu[dpuId] > 0 && resultsPerDpu[dpuId])
        {
            for(uint32_t r = 0; r < resultCountsPerDpu[dpuId]; ++r)
            {
                DpuResult* res = &resultsPerDpu[dpuId][r];

                if(res->leafAddr != 0)
                {
                    KDNode* leaf = resolveNodeLocation(res->leafAddr, dpuId);
                    if(leaf)
                    {
                        (*results) = realloc(*results, (*resultCount + 1) * sizeof(KDNode*));
                        (*resultIndices) = realloc(*resultIndices, (*resultCount + 1) * sizeof(uint32_t));
                        (*results)[*resultCount] = leaf;
                        (*resultIndices)[*resultCount] = (uint32_t)res->batchIndex;
                        (*resultCount)++;
                    }
                }
            }

            free(resultsPerDpu[dpuId]);
        }
    }

    free(resultsPerDpu);
    free(resultCountsPerDpu);

    logMessage("DPU batch offload completed", INFO);
    return true;
}

bool executePartialRebuildForDelete(RebuildInfo* rebuilds, uint32_t rebuildCount, KDTree* tree)
{
    logMessage("Executing partial rebuild for delete", DEBUG);

    if(!rebuilds || rebuildCount == 0 || !tree)
    {
        logMessage("Invalid arguments for partial rebuild on delete", ERROR);
        return false;
    }

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

    char msg[128];
    snprintf(msg, sizeof(msg), "Unique subtrees to rebuild for delete: %u (total rebuild infos: %u)", uniqueCount, rebuildCount);
    logMessage(msg, INFO);

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
                        for(uint8_t d = 0; d < getDimensions(); ++d)
                        {
                            if(fabs(oldPoints[i]->coords[d] - rebuilds[r].deletedPoints[j]->coords[d]) > 0.1)
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

        char localMsg[128];
        snprintf(localMsg, sizeof(localMsg), "Subtree[%u] filtered: original=%zu, remaining=%zu, removed=%zu", u, oldPointsCount, filteredCount, oldPointsCount - filteredCount);
        logMessage(localMsg, INFO);

        if(filteredCount == 0)
        {
            KDNode* emptyLeaf = createLeafNode(NULL, 0);
            if(emptyLeaf)
                replaceSubtree(rootToRebuild, emptyLeaf, tree);
        }
        else
        {
            KDTree* rebuiltSubtree = NULL;

            if(filteredCount > getLeafWrapThreshold() * getNPim())
                rebuiltSubtree = buildPIMkdtree(filteredPoints, filteredCount);
            else
            {
                KDNode* newRoot = buildTree(filteredPoints, filteredCount, 0);
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

                snprintf(localMsg, sizeof(localMsg), "Subtree[%u] rebuilt: delta=%d, newSize=%zu", u, delta, filteredCount);
                logMessage(localMsg, INFO);

                while(parent)
                {
                    if(parent->type == INTERNAL)
                        parent->data.internal.approximateCounter += delta;

                    parent = parent->parent;
                }

                free(rebuiltSubtree);
            }
            else
                logMessage("Failed to rebuild subtree for delete", ERROR);
        }

        free(oldPoints);
        free(filteredPoints);
    }

    free(uniqueRoots);
    free(uniqueCounts);

    logMessage("Partial rebuild for delete completed", INFO);
    return true;
}

bool pointExistsInLeaf(KDNode* leaf, point* p)
{
    logMessage("Checking if point exists in leaf", DEBUG);

    if(!leaf || leaf->type != LEAF)
    {
        logMessage("Invalid or non-leaf node provided", ERROR);
        return false;
    }

    float tolerance = getTolerance();

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
    {
        bool match = true;
        for(uint8_t d = 0; d < getDimensions(); ++d)
        {
            if(fabs(leaf->data.leaf.points[i].coords[d] - p->coords[d]) > tolerance)
            {
                match = false;
                break;
            }
        }

        if(match)
        {
            logMessage("Point found in leaf", INFO);
            return true;
        }
    }

    logMessage("Point not found in leaf", DEBUG);
    return false;
}

SearchBatch* leafSearchForDelete(SearchBatch* batch, KDNode*** imbalancedNodes, bool** pointsFound)
{
    logMessage("Starting leaf search for delete", DEBUG);

    if(!batch || !imbalancedNodes || !pointsFound)
    {
        logMessage("Invalid arguments for leaf search for delete", ERROR);
        return NULL;
    }

    *imbalancedNodes = (KDNode**)calloc(batch->size, sizeof(KDNode*));
    *pointsFound = (bool*)calloc(batch->size, sizeof(bool));

    if(!*imbalancedNodes || !*pointsFound)
    {
        logMessage("Failed to allocate imbalanced nodes or pointsFound arrays", ERROR);
        free(*imbalancedNodes);
        free(*pointsFound);
        return NULL;
    }

    PushPullContext* context = initPushPullContext();
    if(!context)
    {
        logMessage("Failed to initialize push-pull context", ERROR);
        free(*imbalancedNodes);
        free(*pointsFound);
        return NULL;
    }

    KDNode** group1Roots = searchGroup0(batch);
    if(!group1Roots)
    {
        logMessage("Group 0 search returned null roots", ERROR);
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
            batch->results[i] = currentNodes[i];
        }

    logMessage("Point existence check completed", INFO);

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
        {
            logMessage("All nodes reached leaves, stopping delete traversal", INFO);
            break;
        }

        memset(context->groupAccessCounts, 0, context->numGroups * sizeof(uint32_t));
        for(size_t i = 0; i < batch->size; ++i)
            if((*pointsFound)[i] && currentNodes[i] && currentNodes[i]->type == INTERNAL)
                context->groupAccessCounts[groupId]++;

        if(shouldPull(NULL, context->groupAccessCounts[groupId], groupId, context))
        {
            logMessage("Pulling nodes to CPU for delete traversal", DEBUG);

            #pragma omp parallel for
            for(size_t i = 0; i < batch->size; ++i)
            {
                if(!(*pointsFound)[i])
                    continue;

                KDNode* node = currentNodes[i];
                if(!node || node->type == LEAF)
                    continue;

                point* q = batch->queries[i];

                propagateCounterUpdate(node, -1, true);

                if(checkBalanceViolation(node))
                {
                    (*imbalancedNodes)[i] = node;
                    continue;
                }

                if(q->coords[node->data.internal.splitDim] < node->data.internal.splitValue)
                    currentNodes[i] = node->data.internal.left;
                else
                    currentNodes[i] = node->data.internal.right;
            }
        }
        else
        {
            logMessage("Pushing delete operations to DPUs", DEBUG);

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

                batchOps[batchOpsCount].type = BATCH_DELETE;
                batchOps[batchOpsCount].targetNodeAddr = (uint64_t)(uintptr_t)currentNodes[i];
                batchOps[batchOpsCount].pointsCount = 1;
                batchOps[batchOpsCount].pointsAddr = pointsAddr;
                batchOps[batchOpsCount].callbackAddr = (uint64_t)i;
                batchOpsCount++;
            }

            if(batchOpsCount > 0)
            {
                KDNode** results = NULL;
                uint32_t* resultIndices = NULL;
                uint32_t resultCount = 0;
                RebuildInfo* rebuilds = NULL;
                uint32_t rebuildCount = 0;

                if(offloadBatchOperationToDpus(batchOps, batchOpsCount, &results, &resultIndices, &resultCount, &rebuilds, &rebuildCount))
                {
                    for(uint32_t r = 0; r < resultCount; ++r)
                    {
                        uint32_t idx = resultIndices[r];
                        if(idx < batch->size && results[r])
                        {
                            currentNodes[idx] = results[r];
                            batch->results[idx] = results[r];
                        }
                    }

                    for(uint32_t r = 0; r < rebuildCount; ++r)
                    {
                        KDNode* node = rebuilds[r].node;
                        if(node)
                            for(size_t j = 0; j < batch->size; ++j)
                                if(currentNodes[j] == node)
                                    (*imbalancedNodes)[j] = node;
                    }

                    if(rebuildCount > 0)
                    {
                        logMessage("Rebuild required after DPU delete, executing partial rebuild", DEBUG);
                        executePartialRebuildForDelete(rebuilds, rebuildCount, getData()->tree);
                    }

                    free(results);
                    free(resultIndices);
                    free(rebuilds);
                }
                else
                {
                    logMessage("DPU batch operation failed during delete", ERROR);
                    return NULL;
                }

                batchOpsCount = 0;
            }
        }
    }

    free(batchOps);
    free(group1Roots);
    free(currentNodes);
    freePushPullContext(context);

    logMessage("Leaf search for delete completed", INFO);
    return batch;
}

bool reconstructImbalancedSubtreesForDelete(SearchBatch* deleteBatch, KDNode** imbalancedNodes, bool* pointsFound, size_t batchSize)
{
    logMessage("Reconstructing imbalanced subtrees for delete", DEBUG);

    if(!deleteBatch || !imbalancedNodes || !pointsFound)
    {
        logMessage("Invalid arguments for subtree reconstruction on delete", ERROR);
        return false;
    }

    float tolerance = getTolerance();
    KDTree* tree = getData()->tree;

    KDNode** uniqueImbalanced = (KDNode**)malloc(batchSize * sizeof(KDNode*));
    uint32_t* uniqueCounts = (uint32_t*)calloc(batchSize, sizeof(uint32_t));
    point*** pointsToRemove = (point***)malloc(batchSize * sizeof(point**));
    size_t* pointsToRemoveCount = (size_t*)calloc(batchSize, sizeof(size_t));
    size_t uniqueCount = 0;

    if(!uniqueImbalanced || !uniqueCounts || !pointsToRemove || !pointsToRemoveCount)
    {
        logMessage("Failed to allocate reconstruction arrays for delete", ERROR);
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

    char msg[128];
    snprintf(msg, sizeof(msg), "Unique imbalanced subtrees to reconstruct for delete: %zu (batchSize=%zu)", uniqueCount, batchSize);
    logMessage(msg, INFO);

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
                for(uint8_t d = 0; d < getDimensions(); ++d)
                {
                    if(fabs(oldPoints[i]->coords[d] - pointsToRemove[u][j]->coords[d]) > tolerance)
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

        char localMsg[128];
        snprintf(localMsg, sizeof(localMsg), "Imbalanced subtree[%zu]: existing=%zu, toRemove=%zu, remaining=%zu", u, oldPointsCount, pointsToRemoveCount[u], filteredCount);
        logMessage(localMsg, INFO);

        if(filteredCount == 0)
        {
            KDNode* emptyLeaf = createLeafNode(NULL, 0);
            if(emptyLeaf)
                replaceSubtree(imbalancedRoot, emptyLeaf, tree);
        }
        else
        {
            KDTree* rebuiltSubtree = buildPIMkdtree(filteredPoints, filteredCount);
            if(rebuiltSubtree)
            {
                replaceSubtree(imbalancedRoot, rebuiltSubtree->root, tree);
                free(rebuiltSubtree);
                logMessage("Imbalanced subtree reconstructed and replaced after delete", INFO);
            }
            else
                logMessage("Failed to rebuild imbalanced subtree for delete", ERROR);
        }

        free(oldPoints);
        free(filteredPoints);
        free(pointsToRemove[u]);
    }

    free(uniqueImbalanced);
    free(uniqueCounts);
    free(pointsToRemove);
    free(pointsToRemoveCount);

    logMessage("Imbalanced subtree reconstruction for delete completed", INFO);
    return true;
}

bool batchDelete(point** points, size_t batchSize)
{
    logMessage("Starting batch delete", DEBUG);

    if(!points || batchSize == 0)
    {
        logMessage("Invalid arguments for batch delete", ERROR);
        return false;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Batch delete: batchSize=%zu", batchSize);
    logMessage(msg, INFO);

    KDTree* tree = getData()->tree;
    if(!tree)
    {
        logMessage("Tree is null, cannot perform batch delete", ERROR);
        return false;
    }

    KDNode* skeleton = tree->root;

    Bucket* buckets = sievePoints(points, batchSize, skeleton);
    if(!buckets)
    {
        logMessage("Failed to sieve points into buckets for delete", ERROR);
        return false;
    }

    SearchBatch* searchBatch = initSearchBatch(points, batchSize);
    if(!searchBatch)
    {
        logMessage("Failed to initialize search batch for delete", ERROR);
        free(buckets);
        return false;
    }

    KDNode** imbalancedNodes = NULL;
    bool* pointsFound = NULL;
    searchBatch = leafSearchForDelete(searchBatch, &imbalancedNodes, &pointsFound);

    if(!searchBatch || !imbalancedNodes || !pointsFound)
    {
        logMessage("Leaf search for delete failed", ERROR);
        freeSearchBatch(searchBatch);
        free(buckets);
        free(imbalancedNodes);
        free(pointsFound);
        return false;
    }

    uint32_t imbalancedCount = 0;
    size_t foundCount = 0;
    for(size_t i = 0; i < batchSize; ++i)
    {
        if(pointsFound[i])
            ++foundCount;
        if(imbalancedNodes[i] && pointsFound[i])
            ++imbalancedCount;
    }

    snprintf(msg, sizeof(msg), "Delete search results: found=%zu/%zu, imbalanced=%u", foundCount, batchSize, imbalancedCount);
    logMessage(msg, INFO);

    bool reconstructionNeeded = imbalancedCount > 0;

    if(reconstructionNeeded)
    {
        snprintf(msg, sizeof(msg), "Imbalanced nodes detected: %u, starting reconstruction for delete", imbalancedCount);
        logMessage(msg, DEBUG);

        if(!reconstructImbalancedSubtreesForDelete(searchBatch, imbalancedNodes, pointsFound, batchSize))
        {
            logMessage("Reconstruction of imbalanced subtrees failed for delete", ERROR);
            free(imbalancedNodes);
            free(pointsFound);
            freeSearchBatch(searchBatch);
            free(buckets);
            return false;
        }
    }

    uint32_t directRemovals = 0;
    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i] && pointsFound[i] && searchBatch->results[i])
        {
            KDNode* leaf = searchBatch->results[i];
            if(leaf && leaf->type == LEAF && removePointFromLeaf(leaf, points[i]) && leaf->parent)
            {
                propagateCounterUpdate(leaf->parent, - 1, true);
                ++directRemovals;
            }
        }
    }

    size_t removedCount = 0;
    for(size_t i = 0; i < batchSize; ++i)
        if(pointsFound[i])
            ++removedCount;

    tree->totalPoints -= removedCount;

    snprintf(msg, sizeof(msg), "Batch delete results: removed=%zu, directRemovals=%u, newTotalPoints=%u", removedCount, directRemovals, tree->totalPoints);
    logMessage(msg, INFO);

    if(tree->groups)
    {
        for(size_t i = 0; i < batchSize; ++i)
        {
            if(imbalancedNodes[i] && pointsFound[i])
                invalidateReplicasForNode(imbalancedNodes[i], tree->groups);
            else if(pointsFound[i] && searchBatch->results[i])
                invalidateReplicasForNode(searchBatch->results[i], tree->groups);
        }
    }

    free(buckets);
    free(imbalancedNodes);
    free(pointsFound);
    freeSearchBatch(searchBatch);

    logMessage("Batch delete completed", INFO);
    return true;
}

void invalidateReplicasForNode(KDNode* node, KDGroup** groups)
{
    logMessage("Invalidating replicas for node", DEBUG);

    if(!node || !groups)
    {
        logMessage("Null node or groups provided to invalidateReplicasForNode", ERROR);
        return;
    }

    size_t nodeSize = getNodeSize(node);
    int groupId = findGroupForSize(nodeSize, groups);

    if(groupId > 0 && groups[groupId])
    {
        for(size_t i = 0; i < groups[groupId]->replicaCount; ++i)
        {
            if(groups[groupId]->replicas[i]->masterNode == node)
            {
                KDNodeReplica* oldReplica = groups[groupId]->replicas[i];

                KDNodeReplica* newReplica = createReplicaFromMaster(node);
                buildTopDownReplica(node, groups[groupId], newReplica);
                buildBottomUpReplica(node, groups[groupId], newReplica);

                groups[groupId]->replicas[i] = newReplica;
                freeReplica(oldReplica);

                logMessage("Replica invalidated and rebuilt", INFO);
                break;
            }
        }
    }
}

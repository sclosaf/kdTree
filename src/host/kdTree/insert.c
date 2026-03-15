#include <stdlib.h>
#include <string.h>
#include <omp.h>

#include "kdTree/update.h"
#include "kdTree/build.h"
#include "kdTree/search.h"
#include "kdTree/utils.h"
#include "kdTree/counters.h"

#include "environment/init.h"

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

    PushPullContext* context = initPushPullContext();
    if(!context)
    {
        free(*imbalancedNodes);
        *imbalancedNodes = NULL;
        return NULL;
    }

    KDNode** group1Roots = NULL;
    searchGroup0(batch, &group1Roots);
    if(!group1Roots)
    {
        free(*imbalancedNodes);
        *imbalancedNodes = NULL;
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

            batch->results[i].pathLength++;
        }
    }

    for(size_t i = 0; i < batch->size; ++i)
        if(!(*imbalancedNodes)[i] && currentNodes[i] && currentNodes[i]->type == LEAF)
            batch->results[i].leaf = currentNodes[i];
        else if((*imbalancedNodes)[i])
            batch->results[i].leaf = NULL;

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
    size_t uniqueCount = 0;

    for(size_t i = 0; i < batchSize; ++i)
    {
        if(!imbalancedNodes[i])
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
            uniqueCount++;
        }
    }

    for(size_t u = 0; u < uniqueCount; ++u)
    {
        KDNode* imbalancedRoot = uniqueImbalanced[u];

        size_t oldPointsCapacity = 1024;
        size_t oldPointsCount = 0;
        point** oldPoints = (point**)malloc(oldPointsCapacity * sizeof(point*));
        collectPointsFromSubtree(imbalancedRoot, &oldPoints, &oldPointsCount, &oldPointsCapacity);

        size_t newPointsCapacity = uniqueCounts[u];
        size_t newPointsCount = 0;
        point** newPoints = (point**)malloc(newPointsCapacity * sizeof(point*));

        for(size_t i = 0; i < batchSize; ++i)
        {
            if(imbalancedNodes[i] == imbalancedRoot && insertBatch->queries[i])
            {
                if(newPointsCount >= newPointsCapacity)
                {
                    newPointsCapacity *= 2;
                    newPoints = (point**)realloc(newPoints, newPointsCapacity * sizeof(point*));
                }

                newPoints[newPointsCount++] = insertBatch->queries[i];
            }
        }

        size_t totalPoints = oldPointsCount + newPointsCount;
        point** allPoints = (point**)malloc(totalPoints * sizeof(point*));
        memcpy(allPoints, oldPoints, oldPointsCount * sizeof(point*));
        memcpy(allPoints + oldPointsCount, newPoints, newPointsCount * sizeof(point*));

        KDTree* rebuiltSubtree = buildPIMkdtree(allPoints, totalPoints);

        if(!rebuiltSubtree)
        {
            free(oldPoints);
            free(newPoints);
            free(allPoints);
            free(uniqueImbalanced);
            free(uniqueCounts);

            return false;
        }

        replaceSubtree(imbalancedRoot, rebuiltSubtree->root, tree);

        free(oldPoints);
        free(newPoints);
        free(allPoints);
    }

    free(uniqueImbalanced);
    free(uniqueCounts);
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

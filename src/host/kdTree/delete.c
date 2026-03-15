#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <math.h>

#include "kdTree/update.h"
#include "kdTree/build.h"
#include "kdTree/search.h"
#include "kdTree/sieve.h"
#include "kdTree/utils.h"
#include "kdTree/counters.h"
#include "environment/init.h"

static bool pointExistsInLeaf(KDNode* leaf, point* p)
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

static SearchBatch* leafSearchForDelete(SearchBatch* batch, KDNode*** imbalancedNodes, bool** pointsFound, size_t totalTreeSize)
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
    {
        if(currentNodes[i] && currentNodes[i]->type == LEAF)
        {
            (*pointsFound)[i] = pointExistsInLeaf(currentNodes[i], batch->queries[i]);
            batch->results[i].leaf = currentNodes[i];
        }
    }

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

    free(group1Roots);
    free(currentNodes);
    freePushPullContext(context);

    return batch;
}

static bool reconstructImbalancedSubtreesForDelete(SearchBatch* deleteBatch, KDNode** imbalancedNodes, bool* pointsFound, size_t batchSize)
{
    if(!deleteBatch || !imbalancedNodes || !pointsFound)
        return false;

    KDTree* tree = getData()->tree;
    KDNode** uniqueImbalanced = (KDNode**)malloc(batchSize * sizeof(KDNode*));
    uint32_t* uniqueCounts = (uint32_t*)calloc(batchSize, sizeof(uint32_t));
    point*** pointsToRemove = (point***)malloc(batchSize * sizeof(point**));
    size_t* pointsToRemoveCount = (size_t*)calloc(batchSize, sizeof(size_t));
    size_t uniqueCount = 0;

    if(!uniqueImbalanced || !uniqueCounts || !pointsToRemove || !pointsToRemoveCount)
    {
        free(uniqueImbalanced);
        free(uniqueCounts);
        free(pointsToRemove);
        free(pointsToRemoveCount);
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
                ++uniqueCounts[j];
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
            ++uniqueCount;
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

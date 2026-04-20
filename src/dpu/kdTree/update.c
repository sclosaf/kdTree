#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <alloc.h>
#include <stdio.h>

#include "dpu/kdTree/update.h"
#include "dpu/kdTree/utils.h"
#include "dpu/kdTree/types.h"
#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"

static bool shouldUpdate(uint32_t currentValue)
{
    float prob = (float)log2f((float)getNPoint()) / (getBeta() * (float)currentValue);

    if(prob <= 0.0f)
        return false;

    if(prob >= 1.0f)
        return true;

    setupRand();

    float r = (float)rand() / (float)RAND_MAX;
    bool result = r < prob;
    return result;
}

static uint32_t getIncrementAmount(uint32_t currentValue)
{
    float prob = (float)log2f((float)getNPoint()) / (getBeta() * (float)currentValue);
    uint32_t amount = (uint32_t)(1.0f / prob);
    uint32_t result = (amount > 0) ? amount : 1;
    return result;
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

bool incrementApproximateCounter(KDNode* node)
{
    if(!node || node->type != INTERNAL)
    {
        DPU_LOG("incrementApproximateCounter: ERR - Invalid node (node=%p, type=%d)", node, node ? node->type : -1);
        return false;
    }

    uint32_t currentValue = node->data.internal.approximateCounter;

    if(shouldUpdate(currentValue))
    {
        uint32_t increment = getIncrementAmount(currentValue);
        node->data.internal.approximateCounter = currentValue + increment;
        return true;
    }

    DPU_LOG("incrementApproximateCounter: No update performed");
    return false;
}

bool decrementApproximateCounter(KDNode* node)
{
    if(!node || node->type != INTERNAL || node->data.internal.approximateCounter == 0)
    {
        DPU_LOG("decrementApproximateCounter: ERR - Invalid node or counter=0");
        return false;
    }

    uint32_t currentValue = node->data.internal.approximateCounter;

    if(shouldUpdate(currentValue))
    {
        uint32_t decrement = getIncrementAmount(currentValue);
        if(currentValue > decrement)
            node->data.internal.approximateCounter = currentValue - decrement;
        else
            node->data.internal.approximateCounter = 1;

        return true;
    }

    DPU_LOG("decrementApproximateCounter: No update performed");
    return false;
}

bool checkBalanceViolation(KDNode* node)
{
    if(!node || node->type != INTERNAL)
    {
        DPU_LOG("checkBalanceViolation: Not an internal node or NULL");
        return false;
    }

    if(!node->data.internal.left || !node->data.internal.right)
    {
        DPU_LOG("checkBalanceViolation: Missing child (left=%p, right=%p)", node->data.internal.left, node->data.internal.right);
        return true;
    }

    uint32_t leftSize = getNodeSize(node->data.internal.left);
    uint32_t rightSize = getNodeSize(node->data.internal.right);

    if(leftSize == 0 || rightSize == 0)
    {
        DPU_LOG("checkBalanceViolation: Zero-size child detected");
        return true;
    }

    uint32_t larger = (leftSize > rightSize) ? leftSize : rightSize;
    uint32_t smaller = (leftSize > rightSize) ? rightSize : leftSize;

    float ratio = (float)larger / (float)smaller;
    float alpha = 1.0f + getAlpha();
    bool violated = ratio > alpha;

    return violated;
}

void propagateCounterUpdate(KDNode* node, int delta)
{
    if(!node)
    {
        DPU_LOG("propagateCounterUpdate: Node is NULL");
        return;
    }

    KDNode* current = node;
    int level = 0;

    while(current && current->type == INTERNAL)
    {
        if(delta > 0)
            incrementApproximateCounter(current);
        else if(delta < 0)
            decrementApproximateCounter(current);

        current = current->parent;
    }
}

KDNode* findInsertionLeaf(KDNode* node, point p)
{
    if(!node)
    {
        DPU_LOG("findInsertionLeaf: ERR - node is NULL");
        return NULL;
    }

    KDNode* current = node;
    int depth = 0;
    float* coords = p.coords;

    while(current && current->type == INTERNAL)
    {
        propagateCounterUpdate(current, 1);

        if(checkBalanceViolation(current))
        {
            DPU_LOG("findInsertionLeaf: Balance violation detected at depth %d", depth-1);
            return current;
        }

        uint8_t splitDim = current->data.internal.splitDim;
        float splitValue = current->data.internal.splitValue;

        if(coords[splitDim] < splitValue)
            current = current->data.internal.left;
        else
            current = current->data.internal.right;
    }

    if(current)
        DPU_LOG("findInsertionLeaf: Found leaf node at depth %d", depth);
    else
        DPU_LOG("findInsertionLeaf: Reached NULL node");

    return current;
}

KDNode* findDeletionLeaf(KDNode* node, point p)
{
    if(!node)
    {
        DPU_LOG("findDeletionLeaf: ERR - node is NULL");
        return NULL;
    }

    KDNode* current = node;
    int depth = 0;
    float* coords = p.coords;

    while(current && current->type == INTERNAL)
    {
        propagateCounterUpdate(current, -1);

        if(checkBalanceViolation(current))
        {
            DPU_LOG("findDeletionLeaf: Balance violation detected at depth %d", depth-1);
            return current;
        }

        uint8_t splitDim = current->data.internal.splitDim;
        float splitValue = current->data.internal.splitValue;

        if(coords[splitDim] < splitValue)
            current = current->data.internal.left;
        else
            current = current->data.internal.right;
    }

    if(current)
        DPU_LOG("findDeletionLeaf: Found leaf node at depth %d", depth);
    else
        DPU_LOG("findDeletionLeaf: Reached NULL node");

    return current;
}

static bool pointMatches(float* p1, float* p2, uint8_t dimensions, float epsilon)
{
    for(uint8_t d = 0; d < dimensions; ++d)
    {
        float diff = fabsf(p1[d] - p2[d]);
        if(diff > epsilon)
            return false;
    }

    return true;
}

static bool insertPointIntoLeaf(KDNode* leaf, point p, uint8_t dimensions)
{
    if(!leaf || leaf->type != LEAF)
    {
        DPU_LOG("insertPointIntoLeaf: ERR - Not a leaf node");
        return false;
    }

    uint16_t leafWrapThreshold = getLeafWrapThreshold();

    if(leaf->data.leaf.pointsCount >= leafWrapThreshold)
        return false;

    if(leaf->data.leaf.points == NULL)
    {
        leaf->data.leaf.points = (point*)mem_alloc(leafWrapThreshold * sizeof(point));
        if(!leaf->data.leaf.points)
        {
            DPU_LOG("insertPointIntoLeaf: ERR - mem_alloc failed");
            return false;
        }
    }

    leaf->data.leaf.points[leaf->data.leaf.pointsCount].coords = p.coords;
    ++leaf->data.leaf.pointsCount;

    return true;
}

static bool removePointFromLeaf(KDNode* leaf, point p, uint8_t dimensions)
{
    if(!leaf || leaf->type != LEAF || leaf->data.leaf.pointsCount == 0)
    {
        DPU_LOG("removePointFromLeaf: ERR - Invalid leaf or empty");
        return false;
    }

    float epsilon = getTolerance();
    int32_t removeIndex = -1;

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
    {
        if(pointMatches(leaf->data.leaf.points[i].coords, p.coords, dimensions, epsilon))
        {
            removeIndex = i;
            break;
        }
    }

    if(removeIndex == -1)
    {
        DPU_LOG("removePointFromLeaf: Point not found in leaf");
        return false;
    }

    if(removeIndex < (int32_t)(leaf->data.leaf.pointsCount - 1))
    {
        DPU_LOG("removePointFromLeaf: Swapping with last point");
        leaf->data.leaf.points[removeIndex] = leaf->data.leaf.points[leaf->data.leaf.pointsCount - 1];
    }

    --leaf->data.leaf.pointsCount;

    if(leaf->data.leaf.pointsCount == 0)
        leaf->data.leaf.points[leaf->data.leaf.pointsCount].coords = NULL;

    return true;
}

int processBatchOperations(BatchOperation* operations, uint32_t count, BatchResult** results, uint32_t* resultCount)
{
    if(!operations || count == 0 || !results || !resultCount)
    {
        DPU_LOG("processBatchOperations: ERR - Invalid parameters");
        return -1;
    }

    *results = (BatchResult*)mem_alloc(count * sizeof(BatchResult));
    if(!(*results))
    {
        DPU_LOG("processBatchOperations: ERR - mem_alloc failed for results (%u bytes)", count * sizeof(BatchResult));
        return -1;
    }

    *resultCount = 0;

    uint8_t dimensions = getDimensions();
    uint16_t leafWrapThreshold = getLeafWrapThreshold();

    for(uint32_t i = 0; i < count; ++i)
    {
        BatchOperation* op = &operations[i];

        float* coords = (float*)(uintptr_t)op->pointsAddr;
        if(!coords)
        {
            DPU_LOG("processBatchOperations: Skipping op %u - NULL coords", i);
            continue;
        }

        point currentPoint;
        currentPoint.coords = coords;

        KDNode* targetNode = (KDNode*)(uintptr_t)op->targetNodeAddr;
        if(!targetNode)
        {
            DPU_LOG("processBatchOperations: Skipping op %u - NULL targetNode", i);
            continue;
        }

        if(op->type == BATCH_INSERT)
        {
            KDNode* result = findInsertionLeaf(targetNode, currentPoint);

            if(result && result->type == LEAF)
            {
                if(insertPointIntoLeaf(result, currentPoint, dimensions))
                {
                    if(result->parent)
                        propagateCounterUpdate(result->parent, 1);

                    (*results)[*resultCount].batchIndex = op->callbackAddr;
                    (*results)[*resultCount].leafAddr = (uint64_t)(uintptr_t)result;
                    (*results)[*resultCount].imbalancedNodeAddr = 0;
                    (*results)[*resultCount].needsRebuild = false;
                    ++(*resultCount);
                }
                else if(result->data.leaf.pointsCount >= leafWrapThreshold)
                {
                    (*results)[*resultCount].batchIndex = op->callbackAddr;
                    (*results)[*resultCount].leafAddr = 0;
                    (*results)[*resultCount].imbalancedNodeAddr = (uint64_t)(uintptr_t)result;
                    (*results)[*resultCount].needsRebuild = true;
                    ++(*resultCount);
                }
            }
            else if(result && result->type == INTERNAL && checkBalanceViolation(result))
            {
                (*results)[*resultCount].batchIndex = op->callbackAddr;
                (*results)[*resultCount].leafAddr = 0;
                (*results)[*resultCount].imbalancedNodeAddr = (uint64_t)(uintptr_t)result;
                (*results)[*resultCount].needsRebuild = true;
                ++(*resultCount);
            }
        }
        else if(op->type == BATCH_DELETE)
        {
            KDNode* result = findDeletionLeaf(targetNode, currentPoint);

            if(result && result->type == LEAF)
            {
                if(removePointFromLeaf(result, currentPoint, dimensions))
                {
                    if(result->parent)
                        propagateCounterUpdate(result->parent, -1);

                    (*results)[*resultCount].batchIndex = op->callbackAddr;
                    (*results)[*resultCount].leafAddr = (uint64_t)(uintptr_t)result;
                    (*results)[*resultCount].imbalancedNodeAddr = 0;
                    (*results)[*resultCount].needsRebuild = false;
                    ++(*resultCount);
                }
            }
            else if(result && result->type == INTERNAL && checkBalanceViolation(result))
            {
                (*results)[*resultCount].batchIndex = op->callbackAddr;
                (*results)[*resultCount].leafAddr = 0;
                (*results)[*resultCount].imbalancedNodeAddr = (uint64_t)(uintptr_t)result;
                (*results)[*resultCount].needsRebuild = true;
                ++(*resultCount);
            }
        }
    }

    return 0;
}

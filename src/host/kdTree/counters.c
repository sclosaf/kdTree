#include <stdlib.h>
#include <math.h>
#include <stdbool.h>
#include <omp.h>

#include "kdTree/types.h"
#include "kdTree/utils.h"
#include "kdTree/build.h"

#include "environment/init.h"

static bool shouldUpdate(uint32_t currentValue, size_t totalTreeSize)
{
    float prob = log2f((float)totalTreeSize) / (getConfig()->beta * (float)currentValue);

    if(prob <= 0.0f)
        return false;

    if(prob >= 1.0f)
        return true;

    float r = (float)rand() / (float)RAND_MAX;

    return r < prob;
}

static uint32_t getIncrementAmount(uint32_t currentValue, size_t totalTreeSize)
{
    float prob = log2f((float)totalTreeSize) / (getConfig()->beta * (float)currentValue);

    return (uint32_t)(1.0f / prob);
}

bool incrementApproximateCounter(KDNode* node, size_t totalTreeSize)
{
    if(!node || node->type != INTERNAL)
        return false;

    uint32_t currentValue = node->data.internal.approximateCounter;

    if(shouldUpdate(currentValue, totalTreeSize))
    {
        uint32_t increment = getIncrementAmount(currentValue, totalTreeSize);
        node->data.internal.approximateCounter = currentValue + increment;

        return true;
    }

    return false;
}

bool decrementApproximateCounter(KDNode* node, size_t totalTreeSize)
{
    if(!node || node->type != INTERNAL || node->data.internal.approximateCounter == 0)
        return false;

    uint32_t currentValue = node->data.internal.approximateCounter;

    if(shouldUpdate(currentValue, totalTreeSize))
    {
        uint32_t increment = getIncrementAmount(currentValue, totalTreeSize);
        if(currentValue > increment)
            node->data.internal.approximateCounter = currentValue - increment;
        else
            node->data.internal.approximateCounter = 1;

        return true;
    }

    return false;
}

bool checkBalanceViolation(KDNode* node)
{
    if(!node || node->type != INTERNAL)
        return false;

    if(!node->data.internal.left || !node->data.internal.right)
        return true;

    uint32_t leftSize = getNodeSize(node->data.internal.left);
    uint32_t rightSize = getNodeSize(node->data.internal.right);

    uint32_t larger = (leftSize > rightSize) ? leftSize : rightSize;
    uint32_t smaller = (leftSize > rightSize) ? rightSize : leftSize;

    float ratio = (float)larger / (float)smaller;

    return ratio > (1.0f + getConfig()->alpha);
}

void propagateCounterUpdate(KDNode* node, int delta, size_t totalTreeSize, bool lowest)

    if(!node)
        return;

    if(node->type == LEAF)
    {
        propagateCounterUpdate(node->parent, delta, totalTreeSize, true);
        return;
    }

    if(lowest)
    {
        if(delta > 0)
            incrementApproximateCounter(node, totalTreeSize);
        else if(delta < 0)
            decrementApproximateCounter(node, totalTreeSize);

        if(node->parent)
            propagateCounterUpdate(node->parent, delta, totalTreeSize, false);
    }
    else
    {
        if(delta > 0)
            node->data.internal.approximateCounter += delta;
        else if(delta < 0)
        {
            if(node->data.internal.approximateCounter >= (uint32_t)(-delta))
                node->data.internal.approximateCounter -= (uint32_t)(-delta);
            else
                node->data.internal.approximateCounter = 1;
        }

        if(node->parent)
            propagateCounterUpdate(node->parent, delta, totalTreeSize, false);
    }
}

void initializeSubtreeCounters(KDNode* node, size_t totalTreeSize)
{
    if(!node || node->type == LEAF)
        return;

    #pragma omp task
    if(node->data.internal.left)
        initializeSubtreeCounters(node->data.internal.left, totalTreeSize);

    #pragma omp task
    if(node->data.internal.right)
        initializeSubtreeCounters(node->data.internal.right, totalTreeSize);

    #pragma omp taskwait

    uint32_t leftSize = getNodeSize(node->data.internal.left);
    uint32_t rightSize = getNodeSize(node->data.internal.right);

    uint32_t total = leftSize + rightSize;

    node->data.internal.approximateCounter = total;
}

KDNode* findFirstImbalance(KDNode* leaf)
{
    if(!leaf)
        return NULL;

    KDNode* current = leaf;

    while(current)
    {
        if(current->type == INTERNAL && checkBalanceViolation(current))
            return current;

        current = current->parent;
    }

    return NULL;
}

bool verifyCounterConsistency(KDNode* node)
{
    if(!node)
        return true;

    if(node->type == LEAF)
        return true;

    bool leftOk = node->data.internal.left ? verifyCounterConsistency(node->data.internal.left) : true;
    bool rightOk = node->data.internal.right ? verifyCounterConsistency(node->data.internal.right) : true;

    if(!leftOk || !rightOk)
        return false;

    uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
    uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
    uint32_t sum = leftSize + rightSize;

    if(sum == 0)
        return node->data.internal.approximateCounter == 0;

    float ratio = (float)node->data.internal.approximateCounter / (float)sum;
    return ratio >= 0.5f && ratio <= 2.0f;
}

#include <stdlib.h>
#include <math.h>
#include <stdbool.h>
#include <omp.h>

#include "host/kdTree/types.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/build.h"

#include "host/environment/init.h"

#include "host/management/logging.h"

static bool shouldUpdate(uint32_t currentValue)
{
    logMessage("Evaluating probabilistic update condition", DEBUG);

    KDTree* tree = getData()->tree;
    float prob = log2f((float)tree->totalPoints) / (getBeta() * (float)currentValue);

    if(prob <= 0.0f)
    {
        logMessage("Update probability <= 0, skipping update", DEBUG);
        return false;
    }

    if(prob >= 1.0f)
    {
        logMessage("Update probability >= 1, forcing update", DEBUG);
        return true;
    }

    float r = (float)rand() / (float)RAND_MAX;

    char msg[256];
    snprintf(msg, sizeof(msg), "Update probability: %.4f, sampled: %.4f, update: %s", prob, r, r < prob ? "yes" : "no");
    logMessage(msg, DEBUG);

    return r < prob;
}

static uint32_t getIncrementAmount(uint32_t currentValue)
{
    logMessage("Computing increment amount", DEBUG);

    KDTree* tree = getData()->tree;
    float prob = log2f((float)tree->totalPoints) / (getBeta() * (float)currentValue);
    uint32_t increment = (uint32_t)(1.0f / prob);

    char msg[256];
    snprintf(msg, sizeof(msg), "Increment amount: %u for currentValue=%u", increment, currentValue);
    logMessage(msg, DEBUG);

    return increment;
}

bool incrementApproximateCounter(KDNode* node)
{
    logMessage("Incrementing approximate counter", DEBUG);

    if(!node || node->type != INTERNAL)
    {
        logMessage("Invalid node for counter increment", ERROR);
        return false;
    }

    uint32_t currentValue = node->data.internal.approximateCounter;

    if(shouldUpdate(currentValue))
    {
        uint32_t increment = getIncrementAmount(currentValue);
        node->data.internal.approximateCounter = currentValue + increment;

        char msg[256];
        snprintf(msg, sizeof(msg), "Counter incremented: %u -> %u", currentValue, node->data.internal.approximateCounter);
        logMessage(msg, INFO);

        return true;
    }

    logMessage("Counter increment skipped by probability", DEBUG);
    return false;
}

bool decrementApproximateCounter(KDNode* node)
{
    logMessage("Decrementing approximate counter", DEBUG);

    if(!node || node->type != INTERNAL || node->data.internal.approximateCounter == 0)
    {
        logMessage("Invalid node or zero counter, skipping decrement", ERROR);
        return false;
    }

    uint32_t currentValue = node->data.internal.approximateCounter;

    if(shouldUpdate(currentValue))
    {
        uint32_t increment = getIncrementAmount(currentValue);
        uint32_t newValue = (currentValue > increment) ? currentValue - increment : 1;
        node->data.internal.approximateCounter = newValue;

        char msg[256];
        snprintf(msg, sizeof(msg), "Counter decremented: %u -> %u", currentValue, newValue);
        logMessage(msg, INFO);

        return true;
    }

    logMessage("Counter decrement skipped by probability", DEBUG);
    return false;
}

bool checkBalanceViolation(KDNode* node)
{
    logMessage("Checking balance violation", DEBUG);

    if(!node || node->type != INTERNAL)
    {
        logMessage("Invalid node for balance check", ERROR);
        return false;
    }

    if(!node->data.internal.left || !node->data.internal.right)
    {
        logMessage("Missing child node, reporting balance violation", DEBUG);
        return true;
    }

    uint32_t leftSize = getNodeSize(node->data.internal.left);
    uint32_t rightSize = getNodeSize(node->data.internal.right);
    uint32_t larger = (leftSize > rightSize) ? leftSize : rightSize;
    uint32_t smaller = (leftSize > rightSize) ? rightSize : leftSize;
    float ratio = (float)larger / (float)smaller;
    bool violated = ratio > (1.0f + getAlpha());

    char msg[256];
    snprintf(msg, sizeof(msg), "Balance ratio: %.4f (threshold: %.4f), violated: %s", ratio, 1.0f + getAlpha(), violated ? "yes" : "no");
    logMessage(msg, INFO);

    return violated;
}

void propagateCounterUpdate(KDNode* node, int delta, bool lowest)
{
    logMessage("Propagating counter update", DEBUG);

    if(!node)
        return;

    if(node->type == LEAF)
    {
        logMessage("Leaf node reached, propagating to parent", DEBUG);
        propagateCounterUpdate(node->parent, delta, true);
        return;
    }

    if(lowest)
    {
        if(delta > 0)
            incrementApproximateCounter(node);
        else if(delta < 0)
            decrementApproximateCounter(node);

        if(node->parent)
            propagateCounterUpdate(node->parent, delta, false);
    }
    else
    {
        uint32_t before = node->data.internal.approximateCounter;

        if(delta > 0)
            node->data.internal.approximateCounter += delta;
        else if(delta < 0)
        {
            if(node->data.internal.approximateCounter >= (uint32_t)(-delta))
                node->data.internal.approximateCounter -= (uint32_t)(-delta);
            else
                node->data.internal.approximateCounter = 1;
        }

        char msg[256];
        snprintf(msg, sizeof(msg), "Counter updated along path: %u -> %u (delta=%d)", before, node->data.internal.approximateCounter, delta);
        logMessage(msg, DEBUG);

        if(node->parent)
            propagateCounterUpdate(node->parent, delta, false);
    }
}

void initializeSubtreeCounters(KDNode* node)
{
    logMessage("Initializing subtree counters", DEBUG);

    if(!node || node->type == LEAF)
        return;

    #pragma omp task
    if(node->data.internal.left)
        initializeSubtreeCounters(node->data.internal.left);

    #pragma omp task
    if(node->data.internal.right)
        initializeSubtreeCounters(node->data.internal.right);

    #pragma omp taskwait

    uint32_t leftSize = getNodeSize(node->data.internal.left);
    uint32_t rightSize = getNodeSize(node->data.internal.right);
    uint32_t total = leftSize + rightSize;
    node->data.internal.approximateCounter = total;

    char msg[256];
    snprintf(msg, sizeof(msg), "Subtree counter initialized: leftSize=%u, rightSize=%u, total=%u", leftSize, rightSize, total);
    logMessage(msg, INFO);
}

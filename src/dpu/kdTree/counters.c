#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

#include "dpu/kdTree/counters.h"
#include "dpu/kdTree/utils.h"
#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"

void initializeCounters(KDNode* node)
{
    if(!node)
    {
        DPU_LOG("initializeCounters: ERR - node is NULL");
        return;
    }

    if(node->type == LEAF)
    {
        DPU_LOG("initializeCounters: Leaf node (pointsCount=%u) - no counters to initialize", node->data.leaf.pointsCount);
        return;
    }

    initializeCounters(node->data.internal.left);
    initializeCounters(node->data.internal.right);

    uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
    uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
    node->data.internal.approximateCounter = leftSize + rightSize;
}

#include <alloc.h>
#include <stdint.h>
#include <stddef.h>
#include <mram.h>
#include <attributes.h>
#include <stdio.h>

#include "dpu/kdTree/types.h"
#include "dpu/kdTree/utils.h"
#include "dpu/environment/config.h"

#include "dpu/environment/macro.h"

__host uint64_t pushArgs[4];
__host __mram_ptr float* pushQueries;
__host size_t pushResultsSize;
__host __mram_ptr uint64_t* pushResults;

int main()
{
    mem_reset();

    uint64_t nodeAddr = pushArgs[0];
    uint32_t numQueries = (uint32_t)pushArgs[1];
    uint32_t dimensions = (uint32_t)pushArgs[2];
    uint8_t groupId = (uint8_t)pushArgs[3];

    DPU_LOG("DPU: Starting Push - nodeAddr=0x%lx, numQueries=%u, dimensions=%u, groupId=%u", nodeAddr, numQueries, dimensions, groupId);

    if(numQueries == 0 || nodeAddr == 0)
    {
        DPU_LOG("DPU: No queries or invalid nodeAddr, exiting (numQueries=%u, nodeAddr=0x%lx)", numQueries, nodeAddr);
        pushResultsSize = 0;
        return 0;
    }

    KDNode* startNode = (KDNode*)(uintptr_t)nodeAddr;
    uint16_t groupHeight = calculateGroupHeight(groupId);
    uint32_t nPim = getNPim();

    DPU_LOG("DPU: startNode=%p, groupHeight=%u, nPim=%u", startNode, groupHeight, nPim);

    if(!startNode)
    {
        DPU_LOG("DPU: ERR - startNode is NULL");
        pushResultsSize = 0;
        return 1;
    }

    DPU_LOG("DPU: startNode type=%s", (startNode->type == INTERNAL) ? "INTERNAL" : "LEAF");

    size_t queryByteSize = dimensions * sizeof(float);
    size_t alignedQuerySize = (queryByteSize + 7) & ~7;

    DPU_LOG("DPU: Query byte size = %zu, aligned = %zu", queryByteSize, alignedQuerySize);

    float* localQuery = (float*)mem_alloc(alignedQuerySize);
    if(!localQuery)
    {
        DPU_LOG("DPU: ERR - mem_alloc failed for localQuery (%zu bytes)", alignedQuerySize);
        return 1;
    }

    DPU_LOG("DPU: Allocated local query buffer at WRAM");

    uint32_t queriesProcessed = 0;

    for(uint32_t i = 0; i < numQueries; ++i)
    {
        DPU_LOG("DPU: Processing query %u/%u", i + 1, numQueries);

        __mram_ptr uint8_t* qPtr = ((__mram_ptr uint8_t*)pushQueries) + (i * alignedQuerySize);
        DPU_LOG("DPU: Reading query %u from MRAM 0x%x", i, (uint32_t)qPtr);

        mram_read(qPtr, localQuery, alignedQuerySize);

        DPU_LOG("DPU: Query %u coordinates: ", i);
        for(uint32_t d = 0; d < dimensions && d < 4; ++d)
            DPU_LOG("%s%f", (d == 0) ? "" : ", ", localQuery[d]);

        if(dimensions > 4)
            DPU_LOG(", ...");

        DPU_LOG("\n");

        KDNode* current = startNode;
        uint16_t steps = 0;

        DPU_LOG("DPU: Starting traversal from node %p", startNode);

        while(current && current->type == INTERNAL && steps < groupHeight)
        {
            size_t subtreeSize = getNodeSize(current);
            DPU_LOG("DPU: Query %u step %u - subtreeSize=%zu, nPim=%u, threshold=%u",
                   i, steps, subtreeSize, nPim, (subtreeSize < nPim));

            if(subtreeSize < nPim)
            {
                DPU_LOG("DPU: Query %u - subtreeSize < nPim, stopping at depth %u", i, steps);
                break;
            }

            uint8_t splitDim = current->data.internal.splitDim;
            float splitValue = current->data.internal.splitValue;

            DPU_LOG("DPU: Query %u step %u - splitDim=%u, splitValue=%f, query[%u]=%f", i, steps, splitDim, splitValue, splitDim, localQuery[splitDim]);

            if(localQuery[splitDim] < splitValue)
            {
                DPU_LOG("DPU: Query %u step %u - going LEFT", i, steps);
                current = current->data.internal.left;
            }
            else
            {
                DPU_LOG("DPU: Query %u step %u - going RIGHT", i, steps);
                current = current->data.internal.right;
            }

            ++steps;
        }

        if(current && steps >= groupHeight)
            DPU_LOG("DPU: Query %u - reached max steps (groupHeight=%u)", i, groupHeight);
        else if(current && current->type == LEAF)
            DPU_LOG("DPU: Query %u - reached leaf node at step %u", i, steps);
        else if(current && current->type == INTERNAL)
            DPU_LOG("DPU: Query %u - stopped at internal node (subtreeSize=%zu < nPim=%u)", i, getNodeSize(current), nPim);
        else
            DPU_LOG("DPU: Query %u - current node is NULL", i);

        uint64_t resAddr = (uint64_t)(uintptr_t)current;
        DPU_LOG("DPU: Query %u result address = 0x%lx", i, resAddr);

        mram_write(&resAddr, &pushResults[i], sizeof(uint64_t));
        DPU_LOG("DPU: Query %u result written to MRAM at 0x%x", i, (uint32_t)&pushResults[i]);

        queriesProcessed++;
    }

    pushResultsSize = numQueries * sizeof(uint64_t);
    DPU_LOG("DPU: Push task completed - processed %u/%u queries, resultsSize=%zu bytes", queriesProcessed, numQueries, pushResultsSize);

    return 0;
}

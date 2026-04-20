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

__host uint64_t group0Args[3];
__host __mram_ptr float* group0Queries;
__host size_t group0ResultsSize;
__host __mram_ptr uint64_t* group0Results;

int main()
{
    mem_reset();

    uint64_t rootAddr = group0Args[0];
    uint32_t numQueries = (uint32_t)group0Args[1];
    uint32_t dimensions = (uint32_t)group0Args[2];

    DPU_LOG("DPU: Starting Search0 - rootAddr=0x%lx, numQueries=%u, dimensions=%u", rootAddr, numQueries, dimensions);

    if(numQueries == 0 || rootAddr == 0)
    {
        DPU_LOG("DPU: No queries or invalid rootAddr, exiting (numQueries=%u, rootAddr=0x%lx)", numQueries, rootAddr);
        group0ResultsSize = 0;
        return 0;
    }

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

    KDNode* root = (KDNode*)(uintptr_t)rootAddr;
    uint32_t nPim = getNPim();

    DPU_LOG("DPU: root=%p, nPim=%u", root, nPim);

    if(!root)
    {
        DPU_LOG("DPU: ERR - root is NULL");
        group0ResultsSize = 0;
        return 1;
    }

    DPU_LOG("DPU: root type=%s", (root->type == INTERNAL) ? "INTERNAL" : "LEAF");

    uint32_t queriesProcessed = 0;

    for(uint32_t i = 0; i < numQueries; ++i)
    {
        DPU_LOG("DPU: Processing query %u/%u", i + 1, numQueries);

        __mram_ptr uint8_t* queryPtr = ((__mram_ptr uint8_t*)group0Queries) + (i * alignedQuerySize);
        DPU_LOG("DPU: Reading query %u from MRAM 0x%x", i, (uint32_t)queryPtr);

        mram_read(queryPtr, localQuery, alignedQuerySize);

        DPU_LOG("DPU: Query %u coordinates: ", i);
        for(uint32_t d = 0; d < dimensions && d < 4; ++d)
            DPU_LOG("%s%f", (d == 0) ? "" : ", ", localQuery[d]);
        if(dimensions > 4)
            DPU_LOG(", ...");
        DPU_LOG("\n");

        KDNode* current = root;
        int depth = 0;

        DPU_LOG("DPU: Starting traversal from root %p", root);

        while(current && current->type == INTERNAL)
        {
            size_t subtreeSize = getNodeSize(current);
            DPU_LOG("DPU: Query %u depth %d - subtreeSize=%zu, nPim=%u, threshold=%u", i, depth, subtreeSize, nPim, (subtreeSize < nPim));

            if(subtreeSize < nPim)
            {
                DPU_LOG("DPU: Query %u - subtreeSize < nPim, stopping at depth %d", i, depth);
                break;
            }

            uint8_t splitDim = current->data.internal.splitDim;
            float splitValue = current->data.internal.splitValue;

            DPU_LOG("DPU: Query %u depth %d - splitDim=%u, splitValue=%f, query[%u]=%f", i, depth, splitDim, splitValue, splitDim, localQuery[splitDim]);

            if(localQuery[splitDim] < splitValue)
            {
                DPU_LOG("DPU: Query %u depth %d - going LEFT", i, depth);
                current = current->data.internal.left;
            }
            else
            {
                DPU_LOG("DPU: Query %u depth %d - going RIGHT", i, depth);
                current = current->data.internal.right;
            }

            ++depth;
        }

        if(current && current->type == LEAF)
            DPU_LOG("DPU: Query %u - reached leaf node at depth %d", i, depth);
        else if(current && current->type == INTERNAL)
            DPU_LOG("DPU: Query %u - stopped at internal node (subtreeSize=%zu < nPim=%u) at depth %d", i, getNodeSize(current), nPim, depth);
        else if(!current)
            DPU_LOG("DPU: Query %u - current node is NULL at depth %d", i, depth);

        uint64_t resultNodeAddr = (uint64_t)(uintptr_t)current;
        DPU_LOG("DPU: Query %u result address = 0x%lx", i, resultNodeAddr);

        mram_write(&resultNodeAddr, &group0Results[i], sizeof(uint64_t));
        DPU_LOG("DPU: Query %u result written to MRAM at 0x%x", i, (uint32_t)&group0Results[i]);

        queriesProcessed++;
    }

    group0ResultsSize = numQueries * sizeof(uint64_t);
    DPU_LOG("DPU: Search0 completed - processed %u/%u queries, resultsSize=%zu bytes", queriesProcessed, numQueries, group0ResultsSize);

    return 0;
}

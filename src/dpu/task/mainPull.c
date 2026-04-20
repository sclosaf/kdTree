#include <alloc.h>
#include <stdint.h>
#include <stddef.h>
#include <mram.h>
#include <attributes.h>
#include <stdio.h>

#include "dpu/kdTree/types.h"
#include "dpu/kdTree/serialization.h"
#include "dpu/kdTree/utils.h"
#include "dpu/environment/config.h"

#include "dpu/environment/macro.h"

__host uint64_t pullArgs[3];
__host __mram_ptr float* pullQueries;
__host size_t subtreeSize;
__host __mram_ptr uint8_t* subtreeData;
__host size_t directResultsSize;
__host __mram_ptr uint64_t* directResults;

int main()
{
    mem_reset();

    uint64_t nodeAddr = pullArgs[0];
    uint32_t numQueries = (uint32_t)pullArgs[1];
    uint32_t dimensions = (uint32_t)pullArgs[2];

    DPU_LOG("DPU: Starting Pull - nodeAddr=0x%lx, numQueries=%u, dimensions=%u", nodeAddr, numQueries, dimensions);

    KDNode* node = (KDNode*)(uintptr_t)nodeAddr;
    if(!node)
    {
        DPU_LOG("DPU: ERR - node is NULL");
        subtreeSize = 0;
        directResultsSize = 0;
        return 1;
    }

    DPU_LOG("DPU: Node validated, type=%s", (node->type == INTERNAL) ? "INTERNAL" : "LEAF");

    size_t totalSize = getNodeSize(node);
    uint32_t threshold = getNPim() * 2;

    DPU_LOG("DPU: Node totalSize=%zu, threshold=%u (NPim=%u)", totalSize, threshold, getNPim());

    if(totalSize < threshold)
    {
        DPU_LOG("DPU: Node size below threshold - performing serialization");
        directResultsSize = 0;

        DPU_LOG("DPU: Serializing subtree...");
        uint8_t* serializedPtr = serializeTree(node, &subtreeSize);

        DPU_LOG("DPU: Serialization complete - subtreeSize=%zu bytes", subtreeSize);

        if(serializedPtr)
        {
            size_t alignedSubtreeSize = (subtreeSize + 7) & ~7;
            DPU_LOG("DPU: Writing serialized data to MRAM at 0x%x (aligned: %zu bytes)", (uint32_t)subtreeData, alignedSubtreeSize);
            mram_write(serializedPtr, subtreeData, alignedSubtreeSize);
            DPU_LOG("DPU: Subtree data written successfully");
        }
        else
        {
            DPU_LOG("DPU: ERR - serializeTree returned NULL");
            subtreeSize = 0;
        }
    }
    else
    {
        DPU_LOG("DPU: Node size above or equal to threshold - performing direct queries");
        subtreeSize = 0;

        if(numQueries > 0)
        {
            directResultsSize = numQueries * sizeof(uint64_t);
            DPU_LOG("DPU: Direct results buffer size = %zu bytes (%u results)", directResultsSize, numQueries);

            size_t querySize = dimensions * sizeof(float);
            size_t alignedQuerySize = (querySize + 7) & ~7;
            DPU_LOG("DPU: Query size = %zu bytes, aligned = %zu bytes", querySize, alignedQuerySize);

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

                __mram_ptr uint8_t* qPtr = ((__mram_ptr uint8_t*)pullQueries) + (i * alignedQuerySize);
                DPU_LOG("DPU: Reading query %u from MRAM 0x%x", i, (uint32_t)qPtr);
                mram_read(qPtr, localQuery, alignedQuerySize);

                DPU_LOG("DPU: Query %u coordinates: ", i);
                for(uint32_t d = 0; d < dimensions && d < 4; ++d)
                    DPU_LOG("%s%f", (d == 0) ? "" : ", ", localQuery[d]);
                if(dimensions > 4)
                    DPU_LOG(", ...");

                DPU_LOG("\n");

                KDNode* current = node;
                int depth = 0;

                while(current && current->type == INTERNAL)
                {
                    uint8_t splitDim = current->data.internal.splitDim;
                    float splitValue = current->data.internal.splitValue;

                    DPU_LOG("DPU: Query %u depth %d - splitDim=%u, splitValue=%f, query[%u]=%f", i, depth, splitDim, splitValue, splitDim, localQuery[splitDim]);

                    if(localQuery[splitDim] < splitValue)
                    {
                        DPU_LOG("DPU: Query %u going LEFT", i);
                        current = current->data.internal.left;
                    }
                    else
                    {
                        DPU_LOG("DPU: Query %u going RIGHT", i);
                        current = current->data.internal.right;
                    }

                    ++depth;
                }

                uint64_t resAddr = (uint64_t)(uintptr_t)current;
                DPU_LOG("DPU: Query %u reached node at address 0x%lx (type=%s, depth=%d)", i, resAddr, (current && current->type == INTERNAL) ? "INTERNAL" : "LEAF", depth);

                mram_write(&resAddr, &directResults[i], sizeof(uint64_t));
                DPU_LOG("DPU: Query %u result written to MRAM at 0x%x", i, (uint32_t)&directResults[i]);

                ++queriesProcessed;
            }

            DPU_LOG("DPU: Processed %u/%u queries successfully", queriesProcessed, numQueries);
        }
        else
        {
            DPU_LOG("DPU: No queries to process (numQueries=0)");
            directResultsSize = 0;
        }
    }

    DPU_LOG("DPU: Pull task completed - subtreeSize=%zu, directResultsSize=%zu", subtreeSize, directResultsSize);
    return 0;
}

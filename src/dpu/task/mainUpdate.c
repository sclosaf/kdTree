#include <alloc.h>
#include <stdint.h>
#include <stddef.h>
#include <mram.h>
#include <attributes.h>
#include <stdio.h>

#include "dpu/kdTree/update.h"

#include "dpu/environment/macro.h"

__host uint32_t batchCount;
__host uint32_t resultCount;
__host __mram_ptr BatchOperation* batchOperations;
__host __mram_ptr BatchResult* batchResults;
__host uint32_t pendingCount;

int main()
{
    mem_reset();

    DPU_LOG("DPU: Starting Update - batchCount=%u, pendingCount=%u", batchCount, pendingCount);
    DPU_LOG("DPU: batchOperations=%p, batchResults=%p", batchOperations, batchResults);

    if(batchCount == 0)
    {
        DPU_LOG("DPU: No batch operations to process (batchCount=0)");
        return 0;
    }

    size_t opsSize = batchCount * sizeof(BatchOperation);
    size_t alignedOpsSize = (opsSize + 7) & ~7;

    DPU_LOG("DPU: Batch operations size = %zu bytes, aligned = %zu bytes", opsSize, alignedOpsSize);
    DPU_LOG("DPU: Reading %u batch operations from MRAM at 0x%x", batchCount, (uint32_t)batchOperations);

    BatchOperation* localOps = (BatchOperation*)mem_alloc(alignedOpsSize);
    if(!localOps)
    {
        DPU_LOG("DPU: ERR - mem_alloc failed for localOps (%zu bytes)", alignedOpsSize);
        return 1;
    }

    DPU_LOG("DPU: Allocated local operations buffer at WRAM");

    mram_read(batchOperations, localOps, alignedOpsSize);
    DPU_LOG("DPU: Batch operations read successfully");

    DPU_LOG("DPU: First operation - type=%d, pointsAddr=0x%lx, targetNodeAddr=0x%lx, callbackAddr=0x%lx", localOps[0].type, localOps[0].pointsAddr, localOps[0].targetNodeAddr, localOps[0].callbackAddr);
    if(batchCount > 1)
    {
        DPU_LOG("DPU: Last operation - type=%d, pointsAddr=0x%lx, targetNodeAddr=0x%lx, callbackAddr=0x%lx", localOps[batchCount-1].type, localOps[batchCount-1].pointsAddr, localOps[batchCount-1].targetNodeAddr, localOps[batchCount-1].callbackAddr);
    }

    BatchResult* localResults = NULL;
    uint32_t originalResultCount = 0;

    DPU_LOG("DPU: Calling processBatchOperations with batchCount=%u", batchCount);
    int ret = processBatchOperations(localOps, batchCount, &localResults, &resultCount);

    DPU_LOG("DPU: processBatchOperations returned ret=%d, resultCount=%u", ret, resultCount);

    if(ret == 0 && localResults != NULL && resultCount > 0)
    {
        size_t totalResultsSize = resultCount * sizeof(BatchResult);
        size_t alignedResultsSize = (totalResultsSize + 7) & ~7;

        DPU_LOG("DPU: Writing %u results (%zu bytes, aligned %zu) to MRAM at 0x%x", resultCount, totalResultsSize, alignedResultsSize, (uint32_t)batchResults);

        DPU_LOG("DPU: First result - batchIndex=0x%lx, leafAddr=0x%lx, imbalancedNodeAddr=0x%lx, needsRebuild=%u", localResults[0].batchIndex, localResults[0].leafAddr, localResults[0].imbalancedNodeAddr, localResults[0].needsRebuild);

        if(resultCount > 1)
            DPU_LOG("DPU: Last result - batchIndex=0x%lx, leafAddr=0x%lx, imbalancedNodeAddr=0x%lx, needsRebuild=%u", localResults[resultCount-1].batchIndex, localResults[resultCount-1].leafAddr, localResults[resultCount-1].imbalancedNodeAddr, localResults[resultCount-1].needsRebuild);

        mram_write(localResults, batchResults, alignedResultsSize);
        DPU_LOG("DPU: Results written to MRAM successfully");
    }
    else if(ret != 0)
        DPU_LOG("DPU: ERR - processBatchOperations failed with ret=%d", ret);
    else if(localResults == NULL)
        DPU_LOG("DPU: Warning - localResults is NULL");
    else if(resultCount == 0)
        DPU_LOG("DPU: No results generated (resultCount=0)");

    DPU_LOG("DPU: Update task completed, returning %d", ret);
    return ret;
}

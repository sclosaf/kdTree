#include <alloc.h>
#include <stdint.h>
#include <stddef.h>
#include <mram.h>
#include <attributes.h>
#include <stdio.h>

#include "dpu/kdTree/types.h"
#include "dpu/kdTree/distribute.h"
#include "dpu/kdTree/serialization.h"

#include "dpu/environment/macro.h"

__host __mram_ptr ReplicaDistribution* distributions;
__host uint32_t pendingCount;
__host __mram_ptr size_t* replicaSizes;
__host __mram_ptr uint8_t* allReplicasBuffer;
__host uint32_t sketchOffset;
__host __mram_ptr uint8_t* sketchData;
__host size_t sketchSize;
__host uint8_t finalStatus;

int main()
{
    mem_reset();

    finalStatus = 0;

    DPU_LOG("DPU: Starting Distribution - pendingCount=%u, sketchSize=%zu, sketchData=%p", pendingCount, sketchSize, sketchData);
    DPU_LOG("DPU: allReplicasBuffer=%p, replicaSizes=%p, distributions=%p", allReplicasBuffer, replicaSizes, distributions);

    if(pendingCount > 0)
    {
        DPU_LOG("DPU: Processing %u pending replicas", pendingCount);

        __mram_ptr uint8_t* mram_cursor = allReplicasBuffer;
        uint32_t replicasProcessed = 0;

        for(uint32_t i = 0; i < pendingCount; ++i)
        {
            DPU_LOG("DPU: Processing replica %u/%u", i + 1, pendingCount);

            ReplicaDistribution dist;
            size_t curSize;

            DPU_LOG("DPU: Reading distribution[%u] from MRAM 0x%x", i, (uint32_t)&distributions[i]);
            mram_read(&distributions[i], &dist, sizeof(ReplicaDistribution));

            DPU_LOG("DPU: Distribution - groupId=%u, replicaIndex=%u", dist.groupId, dist.replicaIndex);

            uint64_t tempSize64;
            DPU_LOG("DPU: Reading replica size from MRAM 0x%x", (uint32_t)&replicaSizes[i]);
            mram_read(&replicaSizes[i], &tempSize64, sizeof(uint64_t));
            curSize = (size_t)tempSize64;

            DPU_LOG("DPU: Replica size = %zu bytes", curSize);

            if(curSize > 0)
            {
                size_t alignedSize = (curSize + 7) & ~7;
                DPU_LOG("DPU: Allocating %zu bytes (aligned) in WRAM for replica data", alignedSize);

                uint8_t* localBuf = (uint8_t*)mem_alloc(alignedSize);
                if(!localBuf)
                {
                    DPU_LOG("DPU: ERR - mem_alloc failed for localBuf (%zu bytes)", alignedSize);
                    finalStatus = 1;
                    return 1;
                }

                DPU_LOG("DPU: Reading replica data from MRAM 0x%x (%zu bytes aligned)", (uint32_t)mram_cursor, alignedSize);
                mram_read(mram_cursor, localBuf, alignedSize);

                DPU_LOG("DPU: Deserializing replica tree...");
                KDNodeReplica* replica = deserializeReplicaTree(localBuf, curSize);

                if(replica)
                {
                    DPU_LOG("DPU: Storing replica locally (groupId=%u, replicaIndex=%u)", dist.groupId, dist.replicaIndex);
                    int storeResult = storeReplicaLocally(dist.groupId, dist.replicaIndex, replica);
                    if(storeResult)
                    {
                        DPU_LOG("DPU: Replica stored successfully");
                        replicasProcessed++;
                    }
                    else
                    {
                        DPU_LOG("DPU: ERR - Failed to store replica");
                        finalStatus = 1;
                    }
                }
                else
                {
                    DPU_LOG("DPU: ERR - Deserialization failed for replica %u", i);
                    finalStatus = 1;
                }

                mram_cursor += alignedSize;
                DPU_LOG("DPU: MRAM cursor advanced to 0x%x", (uint32_t)mram_cursor);
            }
            else
                DPU_LOG("DPU: Warning - Replica size is 0, skipping");

            if(finalStatus != 0)
            {
                DPU_LOG("DPU: Stopping due to error status");
                break;
            }
        }

        DPU_LOG("DPU: Processed %u/%u replicas successfully", replicasProcessed, pendingCount);
    }
    else
        DPU_LOG("DPU: No pending replicas to process");

    if(sketchSize > 0 && sketchData != NULL)
    {
        DPU_LOG("DPU: Processing sketch - size=%zu bytes, MRAM address=0x%x", sketchSize, (uint32_t)sketchData);

        size_t alignedSketchSize = (sketchSize + 7) & ~7;
        DPU_LOG("DPU: Allocating %zu bytes (aligned) in WRAM for sketch", alignedSketchSize);

        uint8_t* localSketch = (uint8_t*)mem_alloc(alignedSketchSize);
        if(localSketch)
        {
            DPU_LOG("DPU: Reading sketch data from MRAM 0x%x", (uint32_t)sketchData);
            mram_read(sketchData, localSketch, alignedSketchSize);

            DPU_LOG("DPU: Storing sketch locally");
            int storeResult = storeSketchLocally(localSketch, sketchSize);
            if(storeResult)
                DPU_LOG("DPU: Sketch stored successfully");
            else
            {
                DPU_LOG("DPU: ERR - Failed to store sketch");
                finalStatus = 1;
            }
        }
        else
        {
            DPU_LOG("DPU: ERR - mem_alloc failed for localSketch (%zu bytes)", alignedSketchSize);
            finalStatus = 1;
        }
    }
    else
        DPU_LOG("DPU: No sketch data to process (sketchSize=%zu, sketchData=%p)", sketchSize, sketchData);

    DPU_LOG("DPU: Distribution task completed, finalStatus=%u", finalStatus);
    return finalStatus;
}

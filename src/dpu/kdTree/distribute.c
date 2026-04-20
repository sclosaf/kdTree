#include <alloc.h>
#include <stdio.h>

#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"

#include "dpu/kdTree/distribute.h"
#include "dpu/kdTree/serialization.h"
#include "dpu/kdTree/utils.h"

int storeReplicaLocally(uint8_t groupId, uint32_t replicaIndex, KDNodeReplica* replica)
{
    if(!replica)
    {
        DPU_LOG("storeReplicaLocally: ERR - replica is NULL");
        return 0;
    }

    static KDGroup** localGroups = NULL;
    static uint8_t numGroups = 0;
    static uint8_t groupsInitialized = 0;

    if(!groupsInitialized)
    {
        numGroups = 1;
        size_t threshold = getNPim();

        while(threshold > 1)
        {
            ++numGroups;
            threshold = (size_t)log2f(threshold);
        }

        localGroups = (KDGroup**)mem_alloc(numGroups * sizeof(KDGroup*));
        if(!localGroups)
        {
            DPU_LOG("storeReplicaLocally: ERR - mem_alloc failed for localGroups (%u bytes)", numGroups * sizeof(KDGroup*));
            return 0;
        }

        for(uint8_t i = 0; i < numGroups; ++i)
        {
            localGroups[i] = (KDGroup*)mem_alloc(sizeof(KDGroup));
            if(!localGroups[i])
            {
                DPU_LOG("storeReplicaLocally: ERR - mem_alloc failed for localGroups[%u]", i);
                return 0;
            }

            localGroups[i]->replicas = NULL;
            localGroups[i]->replicaCount = 0;
            localGroups[i]->masterRoots = NULL;
            localGroups[i]->masterRootCount = 0;
        }

        groupsInitialized = 1;
    }

    if(groupId >= numGroups)
    {
        DPU_LOG("storeReplicaLocally: ERR - groupId=%u >= numGroups=%u", groupId, numGroups);
        return 0;
    }

    KDGroup* group = localGroups[groupId];
    if(!group->replicas)
    {
        uint32_t initialCapacity = 64;

        group->replicas = (KDNodeReplica**)mem_alloc(initialCapacity * sizeof(KDNodeReplica*));
        group->masterRoots = (KDNode**)mem_alloc(initialCapacity * sizeof(KDNode*));

        if(!group->replicas || !group->masterRoots)
        {
            DPU_LOG("storeReplicaLocally: ERR - allocation failed for replicas or masterRoots");
            return 0;
        }

        for(uint32_t i = 0; i < initialCapacity; ++i)
        {
            group->replicas[i] = NULL;
            group->masterRoots[i] = NULL;
        }

        group->replicaCount = initialCapacity;
    }

    if(replicaIndex >= group->replicaCount)
    {
        DPU_LOG("storeReplicaLocally: ERR - replicaIndex=%u >= replicaCount=%u", replicaIndex, group->replicaCount);
        return 0;
    }

    group->replicas[replicaIndex] = replica;
    if(replica->masterNode)
    {
        group->masterRoots[replicaIndex] = replica->masterNode;
        if(replicaIndex >= group->masterRootCount)
            group->masterRootCount = replicaIndex + 1;
    }

    return 1;
}

int storeSketchLocally(uint8_t* sketchData, size_t sketchSize)
{
    if(!sketchData || sketchSize == 0)
    {
        DPU_LOG("storeSketchLocally: ERR - Invalid sketchData or sketchSize");
        return 0;
    }

    static KDNode* localSketch = NULL;
    static uint8_t sketchInitialized = 0;

    localSketch = deserializeTree(sketchData, sketchSize);

    if(localSketch != NULL)
        return 1;
    else
    {
        DPU_LOG("storeSketchLocally: ERR - Deserialization failed");
        return 0;
    }
}

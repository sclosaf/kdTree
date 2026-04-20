#ifndef DPU_KDTREE_DISTRIBUTE_H
#define DPU_KDTREE_DISTRIBUTE_H

#include <stdint.h>
#include "dpu/kdTree/types.h"

typedef struct ReplicaDistribution
{
    uint8_t groupId;
    uint32_t replicaIndex;
    uint64_t replicaAddress;
    uint64_t masterAddress;
    size_t replicaSize;
} ReplicaDistribution;

int storeReplicaLocally(uint8_t groupId, uint32_t replicaIndex, KDNodeReplica* replica);
int storeSketchLocally(uint8_t* sketchData, size_t sketchSize);

#endif

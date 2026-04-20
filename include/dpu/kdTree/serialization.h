#ifndef DPU_KDTREE_SERIALIZATION_H
#define DPU_KDTREE_SERIALIZATION_H

#include <stdint.h>
#include "dpu/kdTree/types.h"

size_t calculateSerializedSize(KDNode* node);
void serializeNode(KDNode* node, uint8_t** ptr);
uint8_t* serializeTree(KDNode* root, size_t* size);

size_t calculateReplicaSerializedSize(KDNodeReplica* replica);
void serializeReplicaNode(KDNodeReplica* replica, uint8_t** ptr);
uint8_t* serializeReplicaTree(KDNodeReplica* root, size_t* size);

size_t calculateFullSerializedSize(KDNode* root, KDGroup** groups);
uint8_t* serializeFullTree(KDNode* root, KDGroup** groups, size_t* size);

KDNode* deserializeTree(uint8_t* buffer, size_t size);
KDNode* deserializeNode(uint8_t** ptr, uint8_t* end);

KDNodeReplica* deserializeReplicaTree(uint8_t* buffer, size_t size);
KDNodeReplica* deserializeReplicaNode(uint8_t** ptr);

#endif

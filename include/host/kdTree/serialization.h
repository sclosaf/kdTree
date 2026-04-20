#ifndef KDTREE_SERIALIZATION_H
#define KDTREE_SERIALIZATION_H

#include "host/kdTree/types.h"

void* serializeTree(KDNode* root, size_t* size);
void serializeNodeSize(KDNode* node, size_t* size);
void serializeNodeData(KDNode* node, uint8_t** ptr);

KDNode* deserializeTree(void* data, size_t size);
KDNode* deserializeNode(uint8_t** ptr, uint8_t* end);

void serializeReplicaNode(KDNodeReplica* replica, uint8_t** ptr);
uint8_t* serializeReplicaTree(KDNodeReplica* root, size_t* size);
size_t calculateReplicaSerializedSize(KDNodeReplica* replica);

#endif

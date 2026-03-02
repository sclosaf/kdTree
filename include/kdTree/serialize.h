#ifndef KDTREE_SERIALIZE_H
#define KDTREE_SERIALIZE_H

void serializeNodeData(KDNode* node, uint8_t** ptr);
void serializeNodeSize(KDNode* node, size_t* size);
void* serializeTree(KDNode* root, size_t* size);

#endif

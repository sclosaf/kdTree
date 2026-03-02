#ifndef KDTREE_DESERIALIZE_H
#define KDTREE_DESERIALIZE_H

#include <stddef.h>
#include <stdint.h>

#include "kdTree/types.h"

KDNode* deserializeTree(void* data, size_t size);
KDNode* deserializeNode(uint8_t** ptr, uint8_t* end);

#endif

#ifndef KDTREE_FREE_H
#define KDTREE_FREE_H

#include "kdTree/types.h"

#include "kdTree/distribute.h"

void freeKDTree(KDNode* node);
void freeMatrix(void** matrix, size_t rows);
void freeLeafNode(KDNode* node);

void freeDpuAllocation(DpuAllocation* alloc);

#endif

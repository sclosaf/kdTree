#ifndef KDTREE_FREE_H
#define KDTREE_FREE_H

#include <stdlib.h>

#include "host/kdTree/types.h"
#include "host/kdTree/search.h"

void freeKDTree(KDNode* node);
void freeMatrix(void** matrix, size_t rows);
void freeLeafNode(KDNode* node);
void freeDpuAllocation(DpuAllocation* alloc);
void freeSearchBatch(SearchBatch* batch);
void freePushPullContext(PushPullContext* context);
void freeReplica(KDNodeReplica* replica);
void freeNodeLocationMap();

#endif

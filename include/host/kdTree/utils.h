#ifndef KDTREE_UTILS_H
#define KDTREE_UTILS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "host/kdTree/types.h"

#include "host/environment/init.h"

typedef struct ReplicaInfo
{
    uint32_t dpuId;
    uint32_t nodeOffset;
    uint32_t nodeCount;
    uint8_t groupLevel;
} ReplicaInfo;

float findMedian(point** points, size_t start, size_t end, uint8_t dim);
uint8_t findSplitDim(point** points, size_t start, size_t end);
uint32_t getBucket(KDNode* sketch, point* p);
uint32_t getNodeSize(KDNode* node);
size_t partitionPoints(point** points, size_t start, size_t end, uint8_t dim, float pivot);
int compareByDim(const void* a, const void* b, void* dim);
uint32_t** computePrefixSum(uint32_t** matrix, size_t rows, size_t cols);

int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups);
int findGroupForSize(size_t size, KDGroup** groups);
bool isNodeInSubtree(KDNode* node, KDNode* potentialRoot);
KDGroup* findGroupForNode(KDNode* node);

uint16_t calculateGroupHeight(uint8_t groupId);
uint8_t calculateNumGroups();
KDNode* findLeafNodeByIndex(KDNode* sketch, uint32_t index);

void collectAllPoints(KDNode* node, point*** collector, size_t* count, size_t* capacity);
void collectAllLeaves(KDNode* node, KDNode*** collector, size_t* count, size_t* capacity);

#endif

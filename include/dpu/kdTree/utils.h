#ifndef DPU_KDTREE_UTILS_H
#define DPU_KDTREE_UTILS_H

#include <stddef.h>
#include <stdint.h>

#include "dpu/kdTree/types.h"

#define RAND_MAX 2147483647

uint32_t getNodeSize(KDNode* node);

uint8_t findSplitDim(point** points, size_t start, size_t end);
float findMedian(point** points, size_t start, size_t end, uint8_t dim);
size_t partitionPoints(point** points, size_t start, size_t end, uint8_t dim, float pivot);
uint32_t getBucket(KDNode* sketch, float* point);
int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups);
uint16_t calculateGroupHeight(uint8_t groupId);

uint64_t* buildNodeAddressMap(KDNode* root, size_t* mapSize);

float log2f(float x);
float fabsf(float x);
void setupRand();
uint32_t rand();

#endif

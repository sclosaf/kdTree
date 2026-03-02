#ifndef KDTREE_UTILS_H
#define KDTREE_UTILS_H

float findMedian(point** points, size_t start, size_t end, uint8_t dim);
uint8_t findSplitDim(point** points, size_t start, size_t end);
int16_t findGroup(size_t size, KDGroup** groups, uint8_t numGroups);
uint32_t getBucket(KDNode* sketch, point* p);
size_t parallelPartition(point** points, size_t start, size_t end, uint8_t dim, float pivot);
int compareByDim(const void* a, const void* b, void* dim);
size_t calculateSubtreeSize(KDNode* node);
uint32_t** computePrefixSum(uint32_t** matrix, size_t rows, size_t cols);

#endif

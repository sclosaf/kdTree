#ifndef KDTREE_BUILD_H
#define KDTREE_BUILD_H

#include "kdTree/types.h"

typedef struct Bucket
{
    point** bucket;
    size_t size;
} Bucket;

KDTree* buildOnChip(point** points, size_t size);
KDGroup** logStarDecompose(KDTree* tree);
KDTree* replicate(KDTree* original, KDGroup** groups);
KDTree* buildPIMkdtree(point** points, size_t totalSize);

KDNode* buildReplicatedTree(KDGroup** groups, uint8_t groupLevel);
KDNode* buildTreeParallel(point** points, size_t size, uint16_t depth);
KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, uint16_t depth);
void buildSketch(KDNode** root, point** samples, size_t sampleCount, uint16_t level);
Bucket* sievePoints(point** points, size_t size, KDNode* sketch);
void attachSubtree(KDNode* sketch, uint16_t bucketId, KDNode* subtree);
KDNode* createLeafNode(point** points, size_t size);
void copyNode(KDNode* dest, KDNode* src);
void assignNodesToGroups(KDNode* node, KDGroup** groups, uint8_t numGroups);

void traverseSketchAndAssign(KDNode* sketch, point** points, size_t n, size_t P, point*** perPimPoints, size_t* perPimCounts);

#endif

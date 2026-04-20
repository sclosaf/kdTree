#ifndef KDTREE_BUILD_H
#define KDTREE_BUILD_H

#include "host/kdTree/types.h"

typedef struct Bucket
{
    point** bucket;
    size_t size;
} Bucket;

KDTree* buildOnChip(point** points, size_t size);
KDGroup** logStarDecompose(KDNode* root);
KDTree* replicate(KDTree* original, KDGroup** groups);
KDTree* buildPIMkdtree(point** points, size_t totalSize);

void assignNodesToGroups(KDNode* node, KDGroup** groups, uint8_t numGroups);
void copyNode(KDNode* dest, KDNode* src);

KDNode* buildTree(point** points, size_t size, uint16_t depth);
Bucket* sievePoints(point** points, size_t size, KDNode* sketch);
void buildSketch(KDNode** root, point** samples, size_t sampleCount, uint16_t level);
KDNode* buildTreePlain(point** points, size_t start, size_t end, uint16_t depth);
KDNode* createLeafNode(point** points, size_t size);
void attachSubtree(KDNode* sketch, uint16_t leafIndex, KDNode* subtree);

void traverseSketchAndAssign(KDNode* sketch, point** points, size_t n, point*** perPimPoints, size_t* perPimCounts);

KDNodeReplica* createReplicaFromMaster(KDNode* masterNode);
void buildTopDownReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica);
void buildBottomUpReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica);

void buildGroupReplicas(KDGroup* group);

#endif

#ifndef DPU_KDTREE_BUILD_H
#define DPU_KDTREE_BUILD_H

#include "dpu/kdTree/types.h"
#include <stddef.h>
#include <stdint.h>

typedef struct Bucket
{
    point** points;
    size_t size;
} Bucket;

KDNode* buildOnChip(point** points, size_t size);

KDNode* buildTree(point** points, size_t size, uint16_t depth);
KDNode* buildTreePlain(point** points, size_t start, size_t end, uint16_t depth);
KDNode* createLeafNode(point** points, size_t size);

void buildSketch(KDNode** root, point** points, size_t sampleCount, uint16_t levels);
Bucket* sievePoints(point** points, size_t size, KDNode* sketch);
void attachSubtree(KDNode* sketch, uint16_t bucketId, KDNode* subtree);

KDGroup** logStarDecompose(KDNode* root, size_t totalPoints);
void assignNodesToGroups(KDNode* node, KDGroup** groups, uint8_t numGroups);

void copyNode(KDNode* dest, KDNode* src);

KDNodeReplica* createReplicaFromMaster(KDNode* masterNode);
void buildTopDownReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica);
void buildBottomUpReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica);
void buildGroupReplicas(KDGroup* group);

#endif

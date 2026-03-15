#ifndef KDTREE_UPDATE_H
#define KDTREE_UPDATE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "kdTree/types.h"
#include "kdTree/search.h"

bool batchInsert(point** points, size_t batchSize, size_t totalTreeSize);
SearchBatch* leafSearchForInsert(SearchBatch* batch, KDNode*** imbalancedNodes, size_t totalTreeSize);
bool reconstructImbalancedSubtrees(SearchBatch* insertBatch, KDNode** imbalancedNodes, size_t batchSize);

bool batchDelete(point** points, size_t batchSize, size_t totalTreeSize);
SearchBatch* leafSearchForDelete(SearchBatch* batch, KDNode*** imbalancedNodes, bool** pointsFound, size_t totalTreeSize);
bool reconstructImbalancedSubtreesForDelete(SearchBatch* deleteBatch, KDNode** imbalancedNodes, bool* pointsFound, size_t batchSize);

void collectPointsFromSubtree(KDNode* node, point*** collector, size_t* count, size_t* capacity);
void replaceSubtree(KDNode* oldRoot, KDNode* newRoot, KDTree* tree);
void updateParentPointers(KDNode* node, KDNode* newParent);

KDNode* insertToSkeleton(KDNode* node, Bucket* bucket, uint32_t* insertCounts, uint32_t totalTreeSize, bool* needsRebuild);

#endif

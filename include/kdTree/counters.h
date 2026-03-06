#ifndef KDTREE_COUNTERS_H
#define KDTREE_COUNTERS_H

#include <stdint.h>
#include <stdbool.h>

#include "kdTree/types.h"

bool incrementApproximateCounter(KDNode* node, size_t totalTreeSize);
bool decrementApproximateCounter(KDNode* node, size_t totalTreeSize);
bool isBalanced(KDNode* node);
void propagateCounterUpdate(KDNode* node, int delta, size_t totalTreeSize, bool lowest);

void initializeSubtreeCounters(KDNode* node, size_t totalTreeSize);
void updatePathCounters(KDNode* leaf, size_t totalTreeSize, int delta);
bool verifyCounterConsistency(KDNode* node);

#endif

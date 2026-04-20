#ifndef KDTREE_COUNTERS_H
#define KDTREE_COUNTERS_H

#include <stdlib.h>
#include <stdbool.h>

#include "host/kdTree/types.h"

bool incrementApproximateCounter(KDNode* node);
bool decrementApproximateCounter(KDNode* node);
bool checkBalanceViolation(KDNode* node);
void propagateCounterUpdate(KDNode* node, int delta, bool lowest);
void initializeSubtreeCounters(KDNode* node);

#endif

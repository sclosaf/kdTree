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

#endif

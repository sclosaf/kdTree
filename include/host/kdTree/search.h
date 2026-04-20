#ifndef KDTREE_SEARCH_H
#define KDTREE_SEARCH_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "host/kdTree/types.h"
#include "host/kdTree/search.h"

#include "host/environment/init.h"

typedef struct SearchBatch
{
    point** queries;
    size_t size;
    KDNode** results;
    KDNodeReplica** localReplicas;
} SearchBatch;

typedef struct PushPullContext
{
    uint32_t* groupThresholds;
    uint8_t numGroups;
    uint32_t batchSize;
    uint8_t fanout;
    uint32_t* groupAccessCounts;
    bool* groupPullDecision;
    uint32_t nodeVisits;
    bool forcePull;
} PushPullContext;

SearchBatch* initSearchBatch(point** queries, size_t size);
PushPullContext* initPushPullContext();
void updatePushPullContext(PushPullContext* context);

bool shouldPull(KDNode* node, uint32_t accessCount, uint8_t groupId, PushPullContext* context);
void pushToPim(point** queries, uint32_t count, KDNode* startNode, uint8_t groupId, KDNode** results);
KDNode** pullToCpu(KDNode* node, point** queries, uint32_t count, PushPullContext* context);

KDNode** searchGroup0(SearchBatch* batch);
KDNode* navigateUsingReplica(KDNodeReplica* replica, point* q, uint8_t groupId);
void traverseGroupWithPushPull(SearchBatch* batch, KDNode** currentNodes, uint8_t groupId, PushPullContext* context);

SearchBatch* leafSearch(SearchBatch* batch);

#endif

#ifndef KDTREE_SEARCH_H
#define KDTREE_SEARCH_H

#include <stdint.h>
#include <stddef.h>

#include "kdTree/types.h"
#include "kdTree/search.h"
#include "environment/init.h"

typedef struct NodeLocationMap
{
    KDNode** nodes;
    uint32_t* dpuIds;
    uint64_t* dpuAddresses;
    size_t count;
    size_t capacity;
} NodeLocationMap;

typedef struct SearchResult
{
    KDNode* leaf;
    uint16_t pathLength;
    uint8_t groupsTraversed;
    uint8_t crossGroupTransfers;
    uint32_t dpuAccesses;
} SearchResult;

typedef struct SearchBatch
{
    point** queries;
    size_t size;
    SearchResult* results;
} SearchBatch;

typedef struct PushPullContext
{
    uint16_t* groupThresholds;
    uint8_t numGroups;
    uint32_t batchSize;
    uint8_t fanout;
    uint32_t* groupAccessCounts;
    bool* groupPullDecision;
    uint32_t nodeVisits;
    bool forcePull;
} PushPullContext;

SearchBatch* leafSearch(SearchBatch* batch, KDTree* tree);
SearchBatch* initSearchBatch(point** queries, size_t size);
void freeSearchBatch(SearchBatch* batch);

void searchGroup0(SearchBatch* batch, KDTree* tree, KDNode*** group1Roots);
void traverseGroupWithPushPull(SearchBatch* batch, KDNode** currentNodes, uint8_t groupId, PushPullContext* context);

bool shouldPull(KDNode* node, uint32_t accessCount, uint8_t groupId, PushPullContext* context);
void pushToPim(point** queries, uint32_t count, KDNode* startNode, uint8_t groupId, SearchResult* results);
KDNode** pullToCpu(KDNode* node, point** queries, uint32_t count, PushPullContext* context);

PushPullContext* initPushPullContext(uint32_t batchSize);
void updatePushPullContext(PushPullContext* context);
void freePushPullContext(PushPullContext* context);

void initNodeLocationMap();
void freeNodeLocationMap();
void registerNodeLocation(KDNode* node, uint32_t dpuId, uint64_t dpuAddr);
KDNode* resolveNodeLocation(uint64_t address, uint32_t dpuId);

#endif

#ifndef QUERY_RANGE_H
#define QUERY_RANGE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "host/kdTree/types.h"

typedef struct RangeQuery
{
    float* minBounds;
    float* maxBounds;
} RangeQuery;

typedef struct RangeResult
{
    point** points;
    size_t count;
    size_t capacity;
} RangeResult;

typedef struct RangeBatch
{
    RangeQuery* queries;
    RangeResult* results;
    size_t size;
} RangeBatch;

RangeBatch* initRangeBatch(size_t batchSize);
void freeRangeBatch(RangeBatch* batch);
void freeRangeResult(RangeResult* result);
bool batchRangeQuery(RangeBatch* batch);
RangeResult* rangeQuery(float* minBounds, float* maxBounds);

bool pointInRange(point* p, float* minBounds, float* maxBounds);
bool nodeIntersectsRange(KDNode* node, float* minBounds, float* maxBounds, float* nodeMin, float* nodeMax);
bool nodeFullyContained(KDNode* node, float* nodeMin, float* nodeMax, float* minBounds, float* maxBounds);

#endif

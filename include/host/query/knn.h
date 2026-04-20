#ifndef QUERY_KNN_H
#define QUERY_KNN_H

#include <stdint.h>
#include <stdbool.h>

#include "host/kdTree/types.h"
#include "host/kdTree/search.h"

typedef struct KNNResult
{
    point** neighbors;
    float* distances;
    uint32_t count;
} KNNResult;

typedef struct KNNQueryBatch
{
    point** queries;
    uint32_t size;
    uint32_t k;
    KNNResult* results;
} KNNQueryBatch;

KNNQueryBatch* initKNNBatch(point** queries, uint32_t batchSize, uint32_t k);
void freeKNNBatch(KNNQueryBatch* batch);

KNNQueryBatch* batchKNN(KNNQueryBatch* batch);
KNNQueryBatch* batchApproximateKNN(KNNQueryBatch* batch, float epsilon);

#endif

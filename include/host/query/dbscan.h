#ifndef QUERY_DBSCAN_H
#define QUERY_DBSCAN_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "host/kdTree/types.h"

typedef struct DBSCANConfig
{
    float epsilon;
    uint32_t minPts;
    float gridCellSize;
} DBSCANConfig;

typedef struct DBSCANResult
{
    uint32_t* labels;
    uint32_t numClusters;
    uint32_t numNoise;
    uint32_t numPoints;
} DBSCANResult;

DBSCANResult* dbscan(point** points, uint32_t numPoints, DBSCANConfig* config);
void freeDBSCANResult(DBSCANResult* result);

#endif

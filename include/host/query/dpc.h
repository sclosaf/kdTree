#ifndef QUERY_DPC_H
#define QUERY_DPC_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "host/kdTree/types.h"

typedef struct DPCResult
{
    float* densities;
    float* deltaDistances;
    uint32_t* nearestHigherDensity;
    uint32_t* clusterAssignment;
    uint32_t* clusterCenters;
    uint32_t numCenters;
    uint32_t numPoints;
} DPCResult;

typedef struct DPCConfig
{
    float minDensityThreshold;
    float minDeltaThreshold;
    uint32_t kNeighbors;
} DPCConfig;

DPCResult* densityPeakClustering(point** points, uint32_t numPoints, DPCConfig* config);
void freeDPCResult(DPCResult* result);

#endif

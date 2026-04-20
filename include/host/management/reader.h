#ifndef MANAGEMENT_READER_H
#define MANAGEMENT_READER_H

#include <stdint.h>
#include <stddef.h>

#include "host/kdTree/types.h"

typedef enum Dataset
{
    DEFAULT10MLN,
    DEFAULT1MLN,
    DEFAULT1000,
    DEFAULT100,
    BENCHMARK // TO ADD
} Dataset;

point* readDataset(Dataset dataset);
void freeDataset(point* points);
void printDataset(const point* points, size_t maxPoints);

point* generatePoints(uint32_t numPoints, float min, float max);
point* readRandom(uint32_t numPoints, uint32_t offsetMultiplier);
point* readOffset(Dataset datasetType, uint32_t numPoints, uint32_t offsetMultiplier);

#endif

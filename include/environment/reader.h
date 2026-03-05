#ifndef ENVIRONMENT_READER_H
#define ENVIRONMENT_READER_H

#include <stdint.h>
#include <stddef.h>
#include "kdTree/types.h"

point* readDataset();
void freeDataset(point* points);
void printDataset(const point* points, size_t maxPoints);

#endif

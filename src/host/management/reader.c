#include <stdio.h>
#include <time.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <libgen.h>

#include "host/management/reader.h"
#include "host/management/logging.h"

#include "host/environment/init.h"

static char* getDatasetFilename(Dataset type)
{
    logMessage("Resolving dataset filename", DEBUG);

    switch(type)
    {
        case DEFAULT100:
            return "100Dataset.bin";
        case DEFAULT1000:
            return "1000Dataset.bin";
        case DEFAULT1MLN:
            return "1MlnDataset.bin";
        case DEFAULT10MLN:
            return "10MlnDataset.bin";
        case BENCHMARK:
           return "benchmarkDataset.bin";
        default:
            logMessage("Unknown dataset type, using fallback", INFO);
            return "dataset.bin";
    }
}

point* readDataset(Dataset dataset)
{
    logMessage("Reading dataset from disk", DEBUG);

    uint32_t numPoints = getNPoint();
    uint8_t dimensions = getDimensions();

    char* root = getProjectRoot();
    if(!root)
    {
        logMessage("Project root not found", ERROR);
        return NULL;
    }

    const char* filename = getDatasetFilename(dataset);

    size_t pathLen = strlen(root) + strlen("/data/") + strlen(filename) + 1;
    char* datasetPath = (char*)malloc(pathLen);
    if(!datasetPath)
    {
        logMessage("Path allocation failed", ERROR);
        return NULL;
    }

    snprintf(datasetPath, pathLen, "%s/data/%s", root, filename);

    FILE* file = fopen(datasetPath, "rb");
    free(datasetPath);

    if(!file)
    {
        logMessage("Failed to open dataset file", ERROR);
        return NULL;
    }

    uint32_t totalFloatsNeeded = numPoints * dimensions;

    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);

    uint32_t totalFloatsInFile = fileSize / sizeof(float);

    if(totalFloatsInFile < totalFloatsNeeded)
    {
        logMessage("Dataset file too small", ERROR);
        fclose(file);
        return NULL;
    }

    point* points = (point*)malloc(numPoints * sizeof(point));
    if(!points)
    {
        logMessage("Points allocation failed", ERROR);
        fclose(file);
        return NULL;
    }

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        points[i].coords = (float*)malloc(dimensions * sizeof(float));
        if(!points[i].coords)
        {
            logMessage("Coordinate allocation failed", ERROR);

            for(uint32_t j = 0; j < i; ++j)
                free(points[j].coords);

            free(points);
            fclose(file);
            return NULL;
        }

        size_t coordsRead = fread(points[i].coords, sizeof(float), dimensions, file);
        if(coordsRead != dimensions)
        {
            logMessage("File read error", ERROR);

            for(uint32_t j = 0; j <= i; ++j)
                free(points[j].coords);

            free(points);
            fclose(file);
            return NULL;
        }
    }

    fclose(file);

    logMessage("Dataset successfully loaded", INFO);
    return points;
}

void freeDataset(point* points)
{
    logMessage("Freeing dataset", DEBUG);

    if(!points)
    {
        logMessage("Null dataset pointer", DEBUG);
        return;
    }

    for(uint32_t i = 0; points[i].coords != NULL; ++i)
        free(points[i].coords);

    free(points);

    logMessage("Dataset freed", INFO);
}

void printDataset(const point* points, size_t maxPoints)
{
    logMessage("Printing dataset", DEBUG);

    if(!points)
    {
        logMessage("Null dataset pointer", ERROR);
        return;
    }

    uint32_t numPoints = getNPoint();
    uint8_t dimensions = getDimensions();

    char msg[128];
    snprintf(msg, sizeof(msg), "Points: %u, Dimensions: %u", numPoints, dimensions);
    logMessage(msg, INFO);

    printf("Dataset loaded:\n");
    printf("  Points: %u (from config)\n", numPoints);
    printf("  Dimensions: %u\n", dimensions);

    size_t pointsToPrint = maxPoints < numPoints ? maxPoints : numPoints;

    for(size_t i = 0; i < pointsToPrint; ++i)
    {
        printf("  Point %3zu: [", i);
        for(uint8_t d = 0; d < dimensions; ++d)
        {
            printf("%.4f", points[i].coords[d]);
            if(d < dimensions - 1)
                printf(", ");
        }
        printf("]\n");
    }

    logMessage("Dataset printed", INFO);
}

point* generatePoints(uint32_t numPoints, float min, float max)
{
    logMessage("Generating random points", DEBUG);

    uint8_t dimensions = getDimensions();

    if(numPoints == 0 || min >= max)
    {
        logMessage("Invalid generation parameters", ERROR);
        return NULL;
    }

    srand((unsigned int)time(NULL));

    point* points = (point*)malloc(numPoints * sizeof(point));
    if(!points)
    {
        logMessage("Points allocation failed", ERROR);
        return NULL;
    }

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        points[i].coords = (float*)malloc(dimensions * sizeof(float));
        if(!points[i].coords)
        {
            logMessage("Coordinate allocation failed", ERROR);

            for(uint32_t j = 0; j < i; ++j)
                free(points[j].coords);

            free(points);
            return NULL;
        }

        for(uint8_t d = 0; d < dimensions; ++d)
            points[i].coords[d] = min + ((float)rand() / RAND_MAX) * (max - min);
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Generated points: %u", numPoints);
    logMessage(msg, INFO);

    return points;
}

point* readOffset(Dataset dataset, uint32_t numPoints, uint32_t offsetMultiplier)
{
    logMessage("Reading dataset with offset", DEBUG);

    if(numPoints == 0)
    {
        logMessage("Invalid number of points", ERROR);
        return NULL;
    }

    uint8_t dimensions = getDimensions();
    uint32_t totalFloatsNeeded = numPoints * dimensions;

    char* root = getProjectRoot();
    if(!root)
    {
        logMessage("Project root not found", ERROR);
        return NULL;
    }

    const char* filename = getDatasetFilename(dataset);

    size_t pathLen = strlen(root) + strlen("/data/") + strlen(filename) + 1;
    char* datasetPath = (char*)malloc(pathLen);
    if(!datasetPath)
    {
        logMessage("Path allocation failed", ERROR);
        return NULL;
    }

    snprintf(datasetPath, pathLen, "%s/data/%s", root, filename);

    FILE* file = fopen(datasetPath, "rb");
    free(datasetPath);

    if(!file)
    {
        logMessage("Failed to open dataset file", ERROR);
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);

    uint32_t totalFloatsInFile = fileSize / sizeof(float);
    uint32_t offset = offsetMultiplier * totalFloatsNeeded;

    if(offset >= totalFloatsInFile || fseek(file, offset * sizeof(float), SEEK_SET) != 0)
    {
        logMessage("Invalid offset", ERROR);
        fclose(file);
        return NULL;
    }

    point* points = (point*)malloc(numPoints * sizeof(point));
    if(!points)
    {
        logMessage("Points allocation failed", ERROR);
        fclose(file);
        return NULL;
    }

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        points[i].coords = (float*)malloc(dimensions * sizeof(float));
        if(!points[i].coords)
        {
            logMessage("Coordinate allocation failed", ERROR);

            for(uint32_t j = 0; j < i; ++j)
                free(points[j].coords);

            free(points);
            fclose(file);
            return NULL;
        }

        size_t coordsRead = fread(points[i].coords, sizeof(float), dimensions, file);
        if(coordsRead != dimensions)
        {
            logMessage("File read error", ERROR);

            for(uint32_t j = 0; j <= i; ++j)
                free(points[j].coords);

            free(points);
            fclose(file);
            return NULL;
        }
    }

    fclose(file);

    logMessage("Offset dataset loaded", INFO);
    return points;
}

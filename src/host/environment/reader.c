#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <libgen.h>

#include "environment/reader.h"
#include "environment/init.h"

point* readDataset()
{
    uint32_t numPoints = getConfig()->nPoint;
    uint8_t dimensions = getConfig()->dimensions;

    char* root = getProjectRoot();

    if(!root)
        return NULL;

    char datasetPath[PATH_MAX];
    snprintf(datasetPath, sizeof(datasetPath), "%s/data/dataset.bin", root);

    FILE* file = fopen(datasetPath, "rb");
    if(!file)
        return NULL;

    uint32_t fileNumPoints, fileDimensions;
    if(fread(&fileNumPoints, sizeof(uint32_t), 1, file) != 1)
    {
        fclose(file);
        return NULL;
    }

    if(fread(&fileDimensions, sizeof(uint32_t), 1, file) != 1)
    {
        fclose(file);
        return NULL;
    }

    if(fileNumPoints != numPoints ||  fileDimensions != dimensions) {
        fclose(file);
        return NULL;
    }

    point* points = (point*)malloc(numPoints * sizeof(point));
    if(!points)
    {
        fclose(file);
        return NULL;
    }

    for(size_t i = 0; i < numPoints; ++i)
    {
        points[i].coords = (float*)malloc(dimensions * sizeof(float));
        if(!points[i].coords)
        {
            for(size_t j = 0; j < i; ++j)
                free(points[j].coords);

            free(points);
            fclose(file);

            return NULL;
        }

        size_t coordsRead = fread(points[i].coords, sizeof(float), dimensions, file);
        if(coordsRead != dimensions)
        {
            for(size_t j = 0; j <= i; ++j)
                free(points[j].coords);

            free(points);
            fclose(file);

            return NULL;
        }
    }

    fclose(file);
    return points;
}

void freeDataset(point* points)
{
    if(points)
    {
        if(points[0].coords)
            free(points[0].coords);

        free(points);
    }
}

void printDataset(const point* points, size_t maxPoints)
{
    if(!points)
        return;

    uint32_t numPoints = getConfig()->nPoint;
    uint8_t dimensions = getConfig()->dimensions;

    printf("Dataset loaded:\n");
    printf("  Points: %u (from config)\n", numPoints);
    printf("  Dimensions: %u\n", dimensions);
    printf("\nFirst %zu points:\n", maxPoints < numPoints ? maxPoints : numPoints);

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
}

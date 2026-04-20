#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>
#include "host/query/dpc.h"
#include "host/query/knn.h"
#include "host/kdTree/search.h"
#include "host/kdTree/utils.h"
#include "host/environment/init.h"
#include "host/management/logging.h"

static float computeDistance(point* a, point* b)
{
    uint8_t dims = getDimensions();
    float dist = 0.0f;

    for(uint8_t d = 0; d < dims; ++d)
    {
        float diff = a->coords[d] - b->coords[d];
        dist += diff * diff;
    }

    return sqrtf(dist);
}

static void computeDensities(point** points, uint32_t numPoints, DPCConfig* config, float* densities)
{
    logMessage("Computing densities using kNN)", DEBUG);

    uint32_t k = config->kNeighbors;
    if(k >= numPoints)
        k = numPoints - 1;

    char msg[128];
    snprintf(msg, sizeof(msg), "Using k=%u neighbors for density computation", k);
    logMessage(msg, INFO);

    KNNQueryBatch* knnBatch = initKNNBatch(points, numPoints, k);
    if(!knnBatch)
    {
        logMessage("Failed to initialize KNN batch for density computation", ERROR);
        return;
    }

    logMessage("Executing batch KNN queries", DEBUG);
    knnBatch = batchKNN(knnBatch);

    #pragma omp parallel for
    for(uint32_t i = 0; i < numPoints; ++i)
    {
        float avgDist = 0.0f;
        uint32_t validNeighbors = 0;

        for(uint32_t j = 0; j < knnBatch->results[i].count && j < k; ++j)
        {
            if(knnBatch->results[i].distances[j] > 0)
            {
                avgDist += knnBatch->results[i].distances[j];
                ++validNeighbors;
            }
        }

        if(validNeighbors > 0)
            avgDist /= validNeighbors;

        densities[i] = 1.0f / (avgDist + 1e-9f);
    }

    float minDensity = densities[0], maxDensity = densities[0];
    for(uint32_t i = 1; i < numPoints; ++i)
    {
        if(densities[i] < minDensity)
            minDensity = densities[i];

        if(densities[i] > maxDensity)
            maxDensity = densities[i];
    }

    snprintf(msg, sizeof(msg), "kNN densities computed: range=[%.6f, %.6f], points=%u, k=%u", minDensity, maxDensity, numPoints, k);
    logMessage(msg, INFO);

    freeKNNBatch(knnBatch);
}

static int compareDensityDesc(const void* a, const void* b, void* densities)
{
    float* dens = (float*)densities;
    uint32_t ia = *(uint32_t*)a;
    uint32_t ib = *(uint32_t*)b;

    if(dens[ia] > dens[ib])
        return -1;

    if(dens[ia] < dens[ib])
        return 1;

    return 0;
}

static void computeDeltaDistances(point** points, uint32_t numPoints, float* densities, float* deltaDistances, uint32_t* nearestHigherDensity, uint32_t k)
{
    logMessage("Computing delta distances using kNN", DEBUG);

    char msg[128];
    snprintf(msg, sizeof(msg), "Using k=%u neighbors for delta computation", k);
    logMessage(msg, INFO);

    KNNQueryBatch* knnBatch = initKNNBatch(points, numPoints, k);
    if(!knnBatch)
    {
        logMessage("Failed to initialize KNN batch for delta computation", ERROR);
        return;
    }

    logMessage("Executing batch KNN queries for delta", DEBUG);
    knnBatch = batchKNN(knnBatch);

    #pragma omp parallel for
    for(uint32_t i = 0; i < numPoints; ++i)
    {
        float minDist = INFINITY;
        uint32_t nearest = i;

        for(uint32_t j = 0; j < knnBatch->results[i].count; ++j)
        {
            if(!knnBatch->results[i].neighbors[j])
                continue;

            uint32_t neighborIndex = 0;
            for(uint32_t t = 0; t < numPoints; ++t)
            {
                if(points[t] == knnBatch->results[i].neighbors[j])
                {
                    neighborIndex = t;
                    break;
                }
            }

            if(densities[neighborIndex] > densities[i])
            {
                float dist = knnBatch->results[i].distances[j];
                if(dist < minDist)
                {
                    minDist = dist;
                    nearest = neighborIndex;
                }
            }
        }

        if(minDist == INFINITY)
        {
            deltaDistances[i] = INFINITY;
            nearestHigherDensity[i] = i;
        }
        else
        {
            deltaDistances[i] = minDist;
            nearestHigherDensity[i] = nearest;
        }
    }

    freeKNNBatch(knnBatch);

    float maxDelta = 0.0f;
    uint32_t maxDeltaCount = 0;
    for(uint32_t i = 0; i < numPoints; ++i)
    {
        if(deltaDistances[i] != INFINITY)
        {
            if(deltaDistances[i] > maxDelta) maxDelta = deltaDistances[i];
            ++maxDeltaCount;
        }
    }

    snprintf(msg, sizeof(msg), "Delta distances computed: max=%.6f, maxDeltaPoints=%u/%u", maxDelta, maxDeltaCount, numPoints);
    logMessage(msg, INFO);
}

static void selectClusterCenters(float* densities, float* deltaDistances, uint32_t numPoints, DPCConfig* config, uint32_t** centers, uint32_t* numCenters)
{
    logMessage("Selecting cluster centers using decision graph", DEBUG);

    float* decisionValues = malloc(numPoints * sizeof(float));
    if(!decisionValues)
    {
        logMessage("Failed to allocate decisionValues", ERROR);
        return;
    }

    float maxDensity = 0.0f;
    float maxDelta = 0.0f;

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        if(densities[i] > maxDensity)
            maxDensity = densities[i];

        if(deltaDistances[i] != INFINITY && deltaDistances[i] > maxDelta)
            maxDelta = deltaDistances[i];
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Normalization: maxDensity=%.6f, maxDelta=%.6f", maxDensity, maxDelta);
    logMessage(msg, INFO);

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        float normDensity = densities[i] / maxDensity;

        if(deltaDistances[i] == INFINITY)
            decisionValues[i] = normDensity;
        else
            decisionValues[i] = normDensity * (deltaDistances[i] / maxDelta);
    }

    uint32_t* indices = malloc(numPoints * sizeof(uint32_t));
    for(uint32_t i = 0; i < numPoints; ++i)
        indices[i] = i;

    qsort_r(indices, numPoints, sizeof(uint32_t), compareDensityDesc, densities);

    *numCenters = 0;
    *centers = malloc(numPoints * sizeof(uint32_t));
    if(!*centers)
    {
        logMessage("Failed to allocate centers array", ERROR);
        free(decisionValues);
        free(indices);
        return;
    }

    float densityThreshold = config->minDensityThreshold;
    if(densityThreshold <= 0.0f)
    {
        float avgDensity = 0.0f;
        for(uint32_t i = 0; i < numPoints; ++i)
            avgDensity += densities[i];

        avgDensity /= numPoints;
        densityThreshold = avgDensity;

        snprintf(msg, sizeof(msg), "Auto density threshold: %.6f (avg=%.6f)", densityThreshold, avgDensity);
        logMessage(msg, INFO);
    }

    float deltaThreshold = config->minDeltaThreshold;
    if(deltaThreshold <= 0.0f)
    {
        float avgDelta = 0.0f;
        uint32_t validCount = 0;
        for(uint32_t i = 0; i < numPoints; ++i)
        {
            if(deltaDistances[i] != INFINITY)
            {
                avgDelta += deltaDistances[i];
                ++validCount;
            }
        }

        if(validCount > 0)
            avgDelta /= validCount;

        deltaThreshold = avgDelta;

        snprintf(msg, sizeof(msg), "Auto delta threshold: %.6f (avg=%.6f)", deltaThreshold, avgDelta);
        logMessage(msg, INFO);
    }

    for(uint32_t index = 0; index < numPoints; ++index)
    {
        uint32_t i = indices[index];

        if(densities[i] >= densityThreshold && deltaDistances[i] >= deltaThreshold)
        {
            (*centers)[(*numCenters)++] = i;
            logMessage("Center selected by density/delta criteria", DEBUG);
        }
        else if(deltaDistances[i] == INFINITY && densities[i] >= densityThreshold * 0.5f)
        {
            (*centers)[(*numCenters)++] = i;
            logMessage("Center selected by infinite delta criteria", DEBUG);
        }
    }

    if(*numCenters == 0 && numPoints > 0)
    {
        (*centers)[0] = indices[0];
        *numCenters = 1;
        logMessage("No centers found by criteria, using highest density point", INFO);
    }

    free(decisionValues);
    free(indices);

    snprintf(msg, sizeof(msg), "Selected %u cluster centers", *numCenters);
    logMessage(msg, INFO);
}

static void assignClusters(point** points, uint32_t numPoints, float* densities, uint32_t* nearestHigherDensity, uint32_t* centers, uint32_t numCenters, uint32_t* clusterAssignment)
{
    logMessage("Assigning points to clusters", DEBUG);

    char msg[128];
    snprintf(msg, sizeof(msg), "Number of centers: %u", numCenters);
    logMessage(msg, INFO);

    uint32_t* sortedIndices = malloc(numPoints * sizeof(uint32_t));
    for(uint32_t i = 0; i < numPoints; ++i)
        sortedIndices[i] = i;

    qsort_r(sortedIndices, numPoints, sizeof(uint32_t), compareDensityDesc, densities);
    logMessage("Points sorted by density for assignment", DEBUG);

    for(uint32_t i = 0; i < numPoints; ++i)
        clusterAssignment[i] = UINT32_MAX;

    for(uint32_t c = 0; c < numCenters; ++c)
        clusterAssignment[centers[c]] = c;

    for(uint32_t index = 0; index < numPoints; ++index)
    {
        uint32_t i = sortedIndices[index];

        if(clusterAssignment[i] != UINT32_MAX)
            continue;

        uint32_t parent = nearestHigherDensity[i];
        while(parent != i && clusterAssignment[parent] == UINT32_MAX)
            parent = nearestHigherDensity[parent];

        if(parent != i && clusterAssignment[parent] != UINT32_MAX)
            clusterAssignment[i] = clusterAssignment[parent];
    }

    uint32_t unassigned = 0;
    for(uint32_t i = 0; i < numPoints; ++i)
    {
        if(clusterAssignment[i] == UINT32_MAX && densities[i] > 0)
        {
            float minDist = INFINITY;
            uint32_t bestCluster = 0;

            for(uint32_t c = 0; c < numCenters; ++c)
            {
                float dist = computeDistance(points[i], points[centers[c]]);
                if(dist < minDist)
                {
                    minDist = dist;
                    bestCluster = c;
                }
            }

            clusterAssignment[i] = bestCluster;
            ++unassigned;
        }
        else if(clusterAssignment[i] == UINT32_MAX)
        {
            clusterAssignment[i] = 0;
            ++unassigned;
        }
    }

    free(sortedIndices);

    if(unassigned > 0)
    {
        snprintf(msg, sizeof(msg), "%u points assigned via nearest center", unassigned);
        logMessage(msg, INFO);
    }

    snprintf(msg, sizeof(msg), "Clusters assigned for %u points", numPoints);
    logMessage(msg, INFO);
}

DPCResult* densityPeakClustering(point** points, uint32_t numPoints, DPCConfig* config)
{
    logMessage("Starting Density Peak Clustering", DEBUG);

    if(!points || numPoints == 0 || !config)
    {
        logMessage("Invalid arguments for DPC", ERROR);
        return NULL;
    }

    if(config->kNeighbors == 0)
    {
        logMessage("kNeighbors must be > 0", ERROR);
        return NULL;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "DPC config: numPoints=%u, k=%u, densityThreshold=%.4f, deltaThreshold=%.4f", numPoints, config->kNeighbors, config->minDensityThreshold, config->minDeltaThreshold);
    logMessage(msg, INFO);

    DPCResult* result = malloc(sizeof(DPCResult));
    if(!result)
    {
        logMessage("Failed to allocate DPC result", ERROR);
        return NULL;
    }

    result->numPoints = numPoints;
    result->densities = malloc(numPoints * sizeof(float));
    result->deltaDistances = malloc(numPoints * sizeof(float));
    result->nearestHigherDensity = malloc(numPoints * sizeof(uint32_t));
    result->clusterAssignment = malloc(numPoints * sizeof(uint32_t));
    result->clusterCenters = NULL;
    result->numCenters = 0;

    if(!result->densities || !result->deltaDistances || !result->nearestHigherDensity || !result->clusterAssignment)
    {
        logMessage("Failed to allocate DPC result arrays", ERROR);
        freeDPCResult(result);
        return NULL;
    }

    logMessage("Computing densities using kNN", INFO);
    computeDensities(points, numPoints, config, result->densities);

    logMessage("Computing delta distances using kNN", INFO);
    computeDeltaDistances(points, numPoints, result->densities, result->deltaDistances, result->nearestHigherDensity, config->kNeighbors);

    logMessage("Selecting cluster centers", DEBUG);
    selectClusterCenters(result->densities, result->deltaDistances, numPoints, config, &result->clusterCenters, &result->numCenters);

    logMessage("Assigning clusters", DEBUG);
    assignClusters(points, numPoints, result->densities, result->nearestHigherDensity, result->clusterCenters, result->numCenters, result->clusterAssignment);

    snprintf(msg, sizeof(msg), "DPC completed: %u clusters from %u points", result->numCenters, numPoints);
    logMessage(msg, INFO);

    return result;
}

void freeDPCResult(DPCResult* result)
{
    if(!result)
    {
        logMessage("Attempted to free NULL DPC result", DEBUG);
        return;
    }

    logMessage("Freeing DPC result", DEBUG);

    if(result->densities)
    {
        free(result->densities);
        logMessage("Densities freed", DEBUG);
    }

    if(result->deltaDistances)
    {
        free(result->deltaDistances);
        logMessage("Delta distances freed", DEBUG);
    }

    if(result->nearestHigherDensity)
    {
        free(result->nearestHigherDensity);
        logMessage("Nearest higher density array freed", DEBUG);
    }

    if(result->clusterAssignment)
    {
        free(result->clusterAssignment);
        logMessage("Cluster assignment freed", DEBUG);
    }

    if(result->clusterCenters)
    {
        free(result->clusterCenters);
        logMessage("Cluster centers freed", DEBUG);
    }

    free(result);
    logMessage("DPC result freed completely", DEBUG);
}

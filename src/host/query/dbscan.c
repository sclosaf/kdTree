#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>
#include <stdbool.h>

#include "host/query/dbscan.h"
#include "host/query/range.h"
#include "host/kdTree/search.h"
#include "host/kdTree/build.h"
#include "host/kdTree/utils.h"
#include "host/environment/init.h"
#include "host/management/logging.h"

typedef struct GridCell
{
    uint32_t* pointIndices;
    uint32_t count;
    uint32_t capacity;
    uint32_t cellId;
    bool isProcessed;
    uint32_t clusterId;
} GridCell;

typedef struct SpatialGrid
{
    GridCell* cells;
    uint32_t gridWidth;
    uint32_t gridHeight;
    float minX, minY;
    float cellSize;
    uint32_t numCells;
    uint32_t numPoints;
} SpatialGrid;

typedef struct UnionFind
{
    uint32_t* parent;
    uint32_t* rank;
    uint32_t size;
} UnionFind;

typedef struct CorePoint
{
    bool isCore;
    uint32_t* neighbors;
    uint32_t neighborCount;
} CorePoint;

typedef struct WavefrontEntry
{
    uint32_t cellId;
    uint32_t clusterId;
} WavefrontEntry;

static UnionFind* createUnionFind(uint32_t size)
{
    logMessage("Creating Union-Find for DBSCAN clustering", DEBUG);

    UnionFind* uf = malloc(sizeof(UnionFind));
    if(!uf) return NULL;

    uf->parent = malloc(size * sizeof(uint32_t));
    uf->rank = calloc(size, sizeof(uint32_t));
    uf->size = size;

    if(!uf->parent || !uf->rank)
    {
        free(uf->parent);
        free(uf->rank);
        free(uf);
        return NULL;
    }

    for(uint32_t i = 0; i < size; ++i)
        uf->parent[i] = i;

    logMessage("Union-Find created successfully", INFO);
    return uf;
}

static void freeUnionFind(UnionFind* uf)
{
    if(uf)
    {
        free(uf->parent);
        free(uf->rank);
        free(uf);
        logMessage("Union Find freed", DEBUG);
    }
}

static uint32_t find(UnionFind* uf, uint32_t x)
{
    while(uf->parent[x] != x)
    {
        uf->parent[x] = uf->parent[uf->parent[x]];
        x = uf->parent[x];
    }

    return x;
}

static void unionSets(UnionFind* uf, uint32_t x, uint32_t y)
{
    uint32_t rootX = find(uf, x);
    uint32_t rootY = find(uf, y);

    if(rootX == rootY)
        return;

    if(uf->rank[rootX] < uf->rank[rootY])
        uf->parent[rootX] = rootY;
    else if(uf->rank[rootX] > uf->rank[rootY])
        uf->parent[rootY] = rootX;
    else
    {
        uf->parent[rootY] = rootX;
        ++uf->rank[rootX];
    }
}

static uint32_t getCellId(SpatialGrid* grid, float x, float y)
{
    int32_t cx = (int32_t)((x - grid->minX) / grid->cellSize);
    int32_t cy = (int32_t)((y - grid->minY) / grid->cellSize);

    if(cx < 0)
        cx = 0;

    if(cy < 0)
        cy = 0;

    if((uint32_t)cx >= grid->gridWidth)
        cx = grid->gridWidth - 1;

    if((uint32_t)cy >= grid->gridHeight)
        cy = grid->gridHeight - 1;

    return cy * grid->gridWidth + cx;
}

static SpatialGrid* buildSpatialGrid(point** points, uint32_t numPoints, float epsilon)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Building spatial grid for DBSCAN (epsilon=%.4f)", epsilon);
    logMessage(msg, DEBUG);

    if(!points || numPoints == 0 || epsilon <= 0)
        return NULL;

    float minX = points[0]->coords[0];
    float maxX = points[0]->coords[0];
    float minY = points[0]->coords[1];
    float maxY = points[0]->coords[1];

    for(uint32_t i = 1; i < numPoints; ++i)
    {
        if(points[i]->coords[0] < minX)
            minX = points[i]->coords[0];

        if(points[i]->coords[0] > maxX)
            maxX = points[i]->coords[0];

        if(points[i]->coords[1] < minY)
            minY = points[i]->coords[1];

        if(points[i]->coords[1] > maxY)
            maxY = points[i]->coords[1];
    }

    float padding = epsilon * 0.01f;
    minX -= padding;
    maxX += padding;
    minY -= padding;
    maxY += padding;

    uint32_t gridWidth = (uint32_t)ceil((maxX - minX) / epsilon) + 1;
    uint32_t gridHeight = (uint32_t)ceil((maxY - minY) / epsilon) + 1;
    uint32_t numCells = gridWidth * gridHeight;

    snprintf(msg, sizeof(msg), "Grid: %u x %u = %u cells, cellSize=%.4f", gridWidth, gridHeight, numCells, epsilon);
    logMessage(msg, INFO);

    SpatialGrid* grid = malloc(sizeof(SpatialGrid));
    if(!grid)
        return NULL;

    grid->cells = calloc(numCells, sizeof(GridCell));
    grid->gridWidth = gridWidth;
    grid->gridHeight = gridHeight;
    grid->minX = minX;
    grid->minY = minY;
    grid->cellSize = epsilon;
    grid->numCells = numCells;
    grid->numPoints = numPoints;

    if(!grid->cells)
    {
        free(grid);
        return NULL;
    }

    uint32_t* counts = calloc(numCells, sizeof(uint32_t));
    if(!counts)
    {
        free(grid->cells);
        free(grid);
        return NULL;
    }

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        uint32_t cellId = getCellId(grid, points[i]->coords[0], points[i]->coords[1]);
        ++counts[cellId];
    }

    for(uint32_t i = 0; i < numCells; ++i)
    {
        if(counts[i] > 0)
        {
            grid->cells[i].pointIndices = malloc(counts[i] * sizeof(uint32_t));
            grid->cells[i].capacity = counts[i];
            grid->cells[i].count = 0;
            grid->cells[i].cellId = i;
            grid->cells[i].isProcessed = false;
            grid->cells[i].clusterId = UINT32_MAX;
        }
    }

    uint32_t* offsets = calloc(numCells, sizeof(uint32_t));
    if(!offsets)
    {
        for(uint32_t i = 0; i < numCells; ++i)
            if(grid->cells[i].pointIndices)
                free(grid->cells[i].pointIndices);

        free(grid->cells);
        free(grid);
        free(counts);

        return NULL;
    }

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        uint32_t cellId = getCellId(grid, points[i]->coords[0], points[i]->coords[1]);
        grid->cells[cellId].pointIndices[offsets[cellId]++] = i;
        ++grid->cells[cellId].count;
    }

    free(counts);
    free(offsets);

    logMessage("Spatial grid built successfully", INFO);
    return grid;
}

static void freeSpatialGrid(SpatialGrid* grid)
{
    if(!grid)
        return;

    for(uint32_t i = 0; i < grid->numCells; ++i)
        if(grid->cells[i].pointIndices)
            free(grid->cells[i].pointIndices);

    free(grid->cells);
    free(grid);
    logMessage("Spatial grid freed", DEBUG);
}

static bool isCorePoint(uint32_t pointIndex, point** points, SpatialGrid* grid, float epsilon, uint32_t minPts, uint32_t** outNeighbors, uint32_t* outNeighborCount)
{
    if(!points || !grid || pointIndex>= grid->numPoints)
    {
        logMessage("Invalid arguments to isCorePoint", ERROR);
        return false;
    }

    point* p = points[pointIndex];

    uint32_t centerCell = getCellId(grid, p->coords[0], p->coords[1]);
    int32_t cx = centerCell % grid->gridWidth;
    int32_t cy = centerCell / grid->gridWidth;
    uint32_t estimatedPoints = 0;

    for(int32_t dy = -1; dy <= 1; ++dy)
        for(int32_t dx = -1; dx <= 1; ++dx)
        {
            int32_t nx = cx + dx, ny = cy + dy;
            if(nx >= 0 && nx < (int32_t)grid->gridWidth && ny >= 0 && ny < (int32_t)grid->gridHeight)
            {
                uint32_t neighborId = ny * grid->gridWidth + nx;
                estimatedPoints += grid->cells[neighborId].count;
            }
        }

    if(estimatedPoints < minPts)
    {
        if(outNeighbors)
            *outNeighbors = NULL;

        if(outNeighborCount)
            *outNeighborCount = 0;

        return false;
    }

    float minBounds[2], maxBounds[2];
    minBounds[0] = p->coords[0] - epsilon;
    maxBounds[0] = p->coords[0] + epsilon;
    minBounds[1] = p->coords[1] - epsilon;
    maxBounds[1] = p->coords[1] + epsilon;

    RangeResult* rangeResult = rangeQuery(minBounds, maxBounds);
    if(!rangeResult)
        return false;

    uint32_t* neighbors = malloc(rangeResult->count * sizeof(uint32_t));
    uint32_t neighborCount = 0;

    for(size_t i = 0; i < rangeResult->count; ++i)
    {
        for(uint32_t j = 0; j < grid->numPoints; ++j)
        {
            if(points[j] == rangeResult->points[i])
            {
                neighbors[neighborCount++] = j;
                break;
            }
        }
    }

    freeRangeResult(rangeResult);

    bool isCore = (neighborCount >= minPts);

    if(outNeighbors)
    {
        *outNeighbors = isCore ? neighbors : NULL;
        *outNeighborCount = isCore ? neighborCount : 0;
    }

    if(!isCore && neighbors)
        free(neighbors);

    return isCore;
}

static void processWavefront(SpatialGrid* grid, point** points, uint32_t startCellId, uint32_t clusterId, float epsilon, uint32_t minPts, UnionFind* uf, bool* isCoreArray)
{
    WavefrontEntry* queue = malloc(grid->numCells * sizeof(WavefrontEntry));
    uint32_t queueHead = 0;
    uint32_t queueTail = 0;

    queue[queueTail++] = (WavefrontEntry){startCellId, clusterId};
    grid->cells[startCellId].isProcessed = true;

    while(queueHead < queueTail)
    {
        WavefrontEntry current = queue[queueHead++];
        uint32_t cellId = current.cellId;

        int32_t cellX = cellId % grid->gridWidth;
        int32_t cellY = cellId / grid->gridWidth;

        for(int32_t dy = -1; dy <= 1; ++dy)
        {
            for(int32_t dx = -1; dx <= 1; ++dx)
            {
                int32_t nx = cellX + dx;
                int32_t ny = cellY + dy;

                if(nx < 0 || nx >= (int32_t)grid->gridWidth || ny < 0 || ny >= (int32_t)grid->gridHeight)
                    continue;

                uint32_t neighborId = ny * grid->gridWidth + nx;

                if(grid->cells[neighborId].isProcessed)
                    continue;

                bool hasCore = false;
                for(uint32_t i = 0; i < grid->cells[neighborId].count && !hasCore; ++i)
                {
                    uint32_t pointIndex = grid->cells[neighborId].pointIndices[i];
                    if(isCoreArray[pointIndex])
                        hasCore = true;
                }

                if(hasCore)
                {
                    grid->cells[neighborId].isProcessed = true;
                    queue[queueTail++] = (WavefrontEntry){neighborId, clusterId};

                    for(uint32_t i = 0; i < grid->cells[neighborId].count; ++i)
                    {
                        uint32_t pointIndex = grid->cells[neighborId].pointIndices[i];
                        unionSets(uf, pointIndex, grid->cells[startCellId].pointIndices[0]);
                    }
                }
            }
        }
    }

    free(queue);
}

DBSCANResult* dbscan(point** points, uint32_t numPoints, DBSCANConfig* config)
{
    logMessage("Starting DBSCAN", DEBUG);

    if(!points || numPoints == 0 || !config)
    {
        logMessage("Invalid arguments for DBSCAN", ERROR);
        return NULL;
    }

    if(config->epsilon <= 0.0f || config->minPts == 0)
    {
        logMessage("Invalid DBSCAN parameters", ERROR);
        return NULL;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "DBSCAN: epsilon=%.4f, minPts=%u, points=%u", config->epsilon, config->minPts, numPoints);
    logMessage(msg, INFO);

    SpatialGrid* grid = buildSpatialGrid(points, numPoints, config->epsilon);
    if(!grid)
    {
        logMessage("Failed to build spatial grid", ERROR);
        return NULL;
    }

    logMessage("Identifying core points", DEBUG);

    bool* isCore = calloc(numPoints, sizeof(bool));
    uint32_t** coreNeighbors = calloc(numPoints, sizeof(uint32_t*));
    uint32_t* coreNeighborCounts = calloc(numPoints, sizeof(uint32_t));

    if(!isCore || !coreNeighbors || !coreNeighborCounts)
    {
        logMessage("Failed to allocate core point arrays", ERROR);
        freeSpatialGrid(grid);
        free(isCore);
        free(coreNeighbors);
        free(coreNeighborCounts);
        return NULL;
    }

    uint32_t coreCount = 0;

    #pragma omp parallel for reduction(+:coreCount)
    for(uint32_t cellId = 0; cellId < grid->numCells; ++cellId)
    {
        if(grid->cells[cellId].count == 0)
            continue;

        for(uint32_t i = 0; i < grid->cells[cellId].count; ++i)
        {
            uint32_t pointIndex = grid->cells[cellId].pointIndices[i];

            uint32_t* neighbors = NULL;
            uint32_t neighborCount = 0;

            if(isCorePoint(pointIndex, points, grid, config->epsilon, config->minPts, &neighbors, &neighborCount))
            {
                isCore[pointIndex] = true;
                coreNeighbors[pointIndex] = neighbors;
                coreNeighborCounts[pointIndex] = neighborCount;
                ++coreCount;
            }
        }
    }

    snprintf(msg, sizeof(msg), "Core points identified: %u/%u", coreCount, numPoints);
    logMessage(msg, INFO);

    logMessage("Building cluster connectivity with wavefront processing", DEBUG);

    UnionFind* uf = createUnionFind(numPoints);
    if(!uf)
    {
        logMessage("Failed to create Union-Find", ERROR);
        freeSpatialGrid(grid);
        free(isCore);
        for(uint32_t i = 0; i < numPoints; ++i)
            if(coreNeighbors[i])
                free(coreNeighbors[i]);

        free(coreNeighbors);
        free(coreNeighborCounts);
        return NULL;
    }

    uint32_t currentCluster = 0;

    for(uint32_t cellId = 0; cellId < grid->numCells; ++cellId)
    {
        if(grid->cells[cellId].count == 0 || grid->cells[cellId].isProcessed)
            continue;

        bool hasCore = false;
        for(uint32_t i = 0; i < grid->cells[cellId].count && !hasCore; ++i)
        {
            uint32_t pointIndex = grid->cells[cellId].pointIndices[i];
            if(isCore[pointIndex])
                hasCore = true;
        }

        if(hasCore)
            processWavefront(grid, points, cellId, currentCluster++, config->epsilon, config->minPts, uf, isCore);
    }

    logMessage("Assigning cluster labels", DEBUG);

    uint32_t* labels = malloc(numPoints * sizeof(uint32_t));
    if(!labels)
    {
        logMessage("Failed to allocate labels array", ERROR);
        freeUnionFind(uf);
        freeSpatialGrid(grid);
        free(isCore);

        for(uint32_t i = 0; i < numPoints; ++i)
            if(coreNeighbors[i])
                free(coreNeighbors[i]);

        free(coreNeighbors);
        free(coreNeighborCounts);

        return NULL;
    }

    for(uint32_t i = 0; i < numPoints; ++i)
        labels[i] = UINT32_MAX;

    uint32_t* rootToCluster = malloc(numPoints * sizeof(uint32_t));
    memset(rootToCluster, 0xFF, numPoints * sizeof(uint32_t));
    uint32_t numClusters = 0;

    for(uint32_t i = 0; i < numPoints; ++i)
    {
        if(!isCore[i])
            continue;

        uint32_t root = find(uf, i);
        if(rootToCluster[root] == UINT32_MAX)
            rootToCluster[root] = numClusters++;

        labels[i] = rootToCluster[root];
    }

    uint32_t borderCount = 0;
    for(uint32_t i = 0; i < numPoints; ++i)
    {
        if(isCore[i] || labels[i] != UINT32_MAX)
            continue;

        float minBounds[2], maxBounds[2];
        minBounds[0] = points[i]->coords[0] - config->epsilon;
        maxBounds[0] = points[i]->coords[0] + config->epsilon;
        minBounds[1] = points[i]->coords[1] - config->epsilon;
        maxBounds[1] = points[i]->coords[1] + config->epsilon;

        RangeResult* rangeResult = rangeQuery(minBounds, maxBounds);
        if(rangeResult)
        {
            for(size_t j = 0; j < rangeResult->count; ++j)
            {
                for(uint32_t k = 0; k < numPoints; ++k)
                {
                    if(points[k] == rangeResult->points[j] && isCore[k])
                    {
                        labels[i] = labels[k];
                        ++borderCount;
                        break;
                    }
                }

                if(labels[i] != UINT32_MAX)
                    break;
            }

            freeRangeResult(rangeResult);
        }
    }

    uint32_t noiseCount = 0;
    for(uint32_t i = 0; i < numPoints; ++i)
        if(labels[i] == UINT32_MAX)
            ++noiseCount;

    snprintf(msg, sizeof(msg), "DBSCAN completed: %u clusters, %u core, %u border, %u noise", numClusters, coreCount, borderCount, noiseCount);
    logMessage(msg, INFO);

    DBSCANResult* result = malloc(sizeof(DBSCANResult));
    if(!result)
    {
        logMessage("Failed to allocate DBSCAN result", ERROR);

        free(labels);
        free(rootToCluster);
        freeUnionFind(uf);
        freeSpatialGrid(grid);
        free(isCore);

        for(uint32_t i = 0; i < numPoints; ++i)
            if(coreNeighbors[i])
                free(coreNeighbors[i]);

        free(coreNeighbors);
        free(coreNeighborCounts);
        return NULL;
    }

    result->labels = labels;
    result->numClusters = numClusters;
    result->numNoise = noiseCount;
    result->numPoints = numPoints;

    for(uint32_t i = 0; i < numPoints; ++i)
        if(coreNeighbors[i])
            free(coreNeighbors[i]);

    free(rootToCluster);
    freeUnionFind(uf);
    freeSpatialGrid(grid);
    free(isCore);
    free(coreNeighbors);
    free(coreNeighborCounts);

    logMessage("DBSCAN execution complete", INFO);
    return result;
}

void freeDBSCANResult(DBSCANResult* result)
{
    if(!result)
        return;

    if(result->labels)
        free(result->labels);

    free(result);
    logMessage("DBSCAN result freed", DEBUG);
}

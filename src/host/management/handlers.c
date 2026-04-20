#include <stdlib.h>
#include <string.h>

#include "host/kdTree/update.h"
#include "host/kdTree/print.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/free.h"

#include "host/query/knn.h"
#include "host/query/range.h"
#include "host/query/dpc.h"
#include "host/query/dbscan.h"

#include "host/environment/init.h"

#include "host/management/logging.h"
#include "host/management/reader.h"
#include "host/management/metrics.h"
#include "host/management/interface.h"
#include "host/management/timer.h"

int handleBuild(void* context)
{
    logMessage("Starting build operation", DEBUG);

    BuildContext* ctx = (BuildContext*)context;
    if(!ctx)
    {
        logMessage("Invalid build context", ERROR);
        return 1;
    }

    int result = 1;

    point* rawPoints = NULL;

    rawPoints = readDataset(ctx->dataset);
    if(!rawPoints)
    {
        logMessage("Dataset loading failed", ERROR);
        return 1;
    }

    if(getData()->tree)
    {
        logMessage("Existing tree detected, freeing", DEBUG);
        freeData();
    }

    uint32_t n = getNPoint();
    point** ptrArray = (point**)malloc(n * sizeof(point*));
    if(!ptrArray)
    {
        logMessage("Failed to allocate pointer array for build", ERROR);
        for(uint32_t i = 0; i < n; ++i)
            free(rawPoints[i].coords);

        free(rawPoints);
        return 1;
    }

    for(uint32_t i = 0; i < n; ++i)
        ptrArray[i] = &rawPoints[i];

    switch(ctx->type)
    {
        case CHIP:
            logMessage("Selected CHIP build", INFO);

            TIME_OP("Build CHIP", { getData()->tree = buildOnChip(ptrArray, getNPoint()); });

            result = (getData()->tree) ? 0 : 1;
            break;

        case PIM:
            logMessage("Selected PIM build", INFO);

            TIME_OP("Build PIM", { getData()->tree = buildPIMkdtree(ptrArray, getNPoint()); });

            result = (getData()->tree) ? 0 : 1;
            break;

        default:
            logMessage("Unknown build type", ERROR);
            result = 1;
            break;
    }

    free(ptrArray);

    if(result != 0)
    {
        logMessage("Build failed", ERROR);
        for(uint32_t i = 0; i < n; ++i)
            free(rawPoints[i].coords);

        free(rawPoints);
    }
    else
        logMessage("Build completed successfully", DEBUG);

    return result;
}

int handleInsert(void* context)
{
    logMessage("Starting insert operation", DEBUG);

    InsertContext* ctx = (InsertContext*)context;
    if(!ctx)
    {
        logMessage("Invalid insert context", ERROR);
        return 1;
    }

    KDTree* tree = getData()->tree;
    if(!tree)
    {
        logMessage("Tree not initialized", ERROR);
        return 1;
    }

    point* points = NULL;
    uint32_t numPoints = 0;
    bool success = false;

    switch(ctx->source)
    {
        case INSERT_FROM_FILE:
            logMessage("Insert source: file", INFO);

            if(ctx->offsetMultiplier == 0)
            {
                points = readDataset(ctx->datasetType);
                if(!points)
                {
                    logMessage("Dataset loading failed", ERROR);
                    break;
                }

                uint32_t totalPoints = getNPoint();

                if(ctx->count > 0 && ctx->count < totalPoints)
                {
                    point* subset = (point*)malloc(ctx->count * sizeof(point));
                    if(!subset)
                    {
                        logMessage("Subset allocation failed", ERROR);
                        freeDataset(points);
                        break;
                    }

                    for(uint32_t i = 0; i < ctx->count; ++i)
                        subset[i].coords = points[i].coords;

                    free(points);
                    points = subset;
                    numPoints = ctx->count;
                }
                else
                    numPoints = totalPoints;
            }
            else
            {
                points = readOffset(ctx->datasetType, ctx->count, ctx->offsetMultiplier);
                if(!points)
                {
                    logMessage("Offset read failed", ERROR);
                    break;
                }

                numPoints = ctx->count;
            }

            success = (points != NULL);
            break;

        case INSERT_RANDOM:
            logMessage("Insert source: random", INFO);

            numPoints = ctx->count;
            points = generatePoints(numPoints, getMinCoord(), getMaxCoord());
            if(!points)
            {
                logMessage("Random generation failed", ERROR);
                break;
            }

            success = true;
            break;

        case INSERT_COORDINATES:
            logMessage("Insert source: coordinates", INFO);

            if(!ctx->point)
            {
                logMessage("Invalid coordinate input", ERROR);
                break;
            }

            numPoints = 1;
            points = (point*)malloc(sizeof(point));
            if(!points)
            {
                logMessage("Point allocation failed", ERROR);
                break;
            }

            points->coords = (float*)malloc(getDimensions() * sizeof(float));
            if(!points->coords)
            {
                logMessage("Coordinate allocation failed", ERROR);
                free(points);
                break;
            }

            memcpy(points->coords, ctx->point->coords, getDimensions() * sizeof(float));
            success = true;
            break;

        default:
            logMessage("Unknown insert source", ERROR);
            break;
    }

    if(!success || !points || numPoints == 0)
    {
        logMessage("Insert preparation failed", ERROR);

        if(points)
            freeDataset(points);

        if(ctx->source == INSERT_COORDINATES && ctx->point)
        {
            free(ctx->point->coords);
            free(ctx->point);
        }

        return 1;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Points to insert: %u", numPoints);
    logMessage(msg, INFO);

    point** pointPtrs = (point**)malloc(numPoints * sizeof(point*));
    for(uint32_t i = 0; i < numPoints; ++i)
        pointPtrs[i] = &points[i];

    TIME_OP("Batch Insert", { success = batchInsert(pointPtrs, numPoints); });

    free(pointPtrs);
    freeDataset(points);

    if(ctx->point)
    {
        free(ctx->point->coords);
        free(ctx->point);
    }


    logMessage(success ? "Insert completed" : "Insert failed", INFO);
    return success ? 0 : 1;
}

int handleDelete(void* context)
{
    logMessage("Starting delete operation", DEBUG);

    DeleteContext* ctx = (DeleteContext*)context;
    if(!ctx)
    {
        logMessage("Invalid delete context", ERROR);
        return 1;
    }

    Data* data = getData();
    if(!data || !data->tree)
    {
        logMessage("Tree not available", ERROR);
        return 1;
    }

    KDTree* tree = data->tree;
    point** pointsToDelete = NULL;
    uint32_t numPoints = 0;
    bool success = false;

    switch(ctx->type)
    {
        case DELETE_COORDINATES:
            logMessage("Delete type: coordinates", INFO);

            pointsToDelete = ctx->point;
            numPoints = ctx->count;

            if(!pointsToDelete || numPoints == 0)
                break;

            SearchBatch* searchBatch = initSearchBatch(pointsToDelete, 1);
            if(!searchBatch)
                break;

            searchBatch = leafSearch(searchBatch);
            if(!searchBatch)
            {
                freeSearchBatch(searchBatch);
                break;
            }

            bool pointFound = false;
            if(searchBatch->results[0])
            {
                KDNode* leaf = searchBatch->results[0];
                if(leaf && leaf->type == LEAF && pointExistsInLeaf(leaf, pointsToDelete[0]))
                    pointFound = true;
            }

            freeSearchBatch(searchBatch);

            if(!pointFound)
            {
                success = true;
                break;
            }

            point** verifiedPointsToDelete = (point**)malloc(sizeof(point*));
            if(!verifiedPointsToDelete)
                break;

            verifiedPointsToDelete[0] = pointsToDelete[0];
            pointsToDelete = verifiedPointsToDelete;
            numPoints = 1;
            break;
        case DELETE_RANDOM:
        {
            logMessage("Delete type: random selection", INFO);

            if(ctx->count == 0)
                break;

            point** allPoints = NULL;
            size_t totalPoints = 0;
            size_t capacity = 0;

            collectAllPoints(tree->root, &allPoints, &totalPoints, &capacity);

            if(totalPoints == 0 || !allPoints)
                break;

            numPoints = (ctx->count < totalPoints) ? ctx->count : totalPoints;
            pointsToDelete = (point**)malloc(numPoints * sizeof(point*));
            if(!pointsToDelete)
            {
                free(allPoints);
                break;
            }

            bool* selected = (bool*)calloc(totalPoints, sizeof(bool));
            if(!selected)
            {
                free(allPoints);
                free(pointsToDelete);
                break;
            }

            uint32_t selectedCount = 0;
            while(selectedCount < numPoints)
            {
                uint32_t index = rand() % totalPoints;
                if(!selected[index])
                {
                    selected[index] = true;
                    pointsToDelete[selectedCount] = allPoints[index];
                    selectedCount++;
                }
            }

            free(selected);
            free(allPoints);

            if(numPoints == 0)
            {
                free(pointsToDelete);
                break;
            }

            break;
        }
        case DELETE_LEAF:
        {
            logMessage("Delete type: leaf-based", INFO);

            if(ctx->leafCount == 0)
                break;

            KDNode** allLeaves = NULL;
            size_t totalLeaves = 0;
            size_t leafCapacity = 0;

            collectAllLeaves(tree->root, &allLeaves, &totalLeaves, &leafCapacity);

            if(totalLeaves == 0 || !allLeaves)
                break;

            uint32_t leavesToDelete = (ctx->leafCount < totalLeaves) ? ctx->leafCount : totalLeaves;
            bool* selectedLeaves = (bool*)calloc(totalLeaves, sizeof(bool));
            if(!selectedLeaves)
            {
                free(allLeaves);
                break;
            }

            uint32_t selectedCount = 0;
            while(selectedCount < leavesToDelete)
            {
                uint32_t index= rand() % totalLeaves;
                if(!selectedLeaves[index])
                {
                    selectedLeaves[index] = true;
                    selectedCount++;
                }
            }

            size_t totalPointsInSelectedLeaves = 0;
            for(size_t i = 0; i < totalLeaves; ++i)
            {
                if(selectedLeaves[i])
                {
                    KDNode* leaf = allLeaves[i];
                    if(leaf && leaf->type == LEAF)
                        totalPointsInSelectedLeaves += leaf->data.leaf.pointsCount;
                }
            }

            if(totalPointsInSelectedLeaves == 0)
            {
                free(selectedLeaves);
                free(allLeaves);
                success = true;
                break;
            }

            pointsToDelete = (point**)malloc(totalPointsInSelectedLeaves * sizeof(point*));
            if(!pointsToDelete)
            {
                free(selectedLeaves);
                free(allLeaves);
                break;
            }

            size_t pointIndex = 0;
            for(size_t i = 0; i < totalLeaves; ++i)
            {
                if(selectedLeaves[i])
                {
                    KDNode* leaf = allLeaves[i];
                    if(leaf && leaf->type == LEAF)
                        for(size_t j = 0; j < leaf->data.leaf.pointsCount; ++j)
                            pointsToDelete[pointIndex++] = &leaf->data.leaf.points[j];
                }
            }

            numPoints = totalPointsInSelectedLeaves;

            free(selectedLeaves);
            free(allLeaves);

            if(numPoints == 0)
            {
                free(pointsToDelete);
                pointsToDelete = NULL;
                success = true;
                break;
            }

            break;
        }
        default:
            logMessage("Unknown delete type", ERROR);
            break;
    }

    if(pointsToDelete && numPoints > 0)
    {
        char msg[128];
        snprintf(msg, sizeof(msg), "Points to delete: %u", numPoints);
        logMessage(msg, INFO);

        TIME_OP("Batch Delete", { success = batchDelete(pointsToDelete, numPoints); });

        free(pointsToDelete);
    }
    else
    {
        logMessage("No points selected for deletion", DEBUG);
        success = true;
    }

    logMessage(success ? "Delete completed" : "Delete failed", INFO);
    return success ? 0 : 1;
}

int handleKNN(void* context)
{
    logMessage("Starting KNN operation", DEBUG);

    KNNContext* ctx = (KNNContext*)context;
    if(!ctx)
    {
        logMessage("Invalid KNN context", ERROR);
        return 1;
    }

    KDTree* tree = getData()->tree;
    if(!tree)
    {
        logMessage("Tree not initialized. Use 'build' command first.", ERROR);
        return 1;
    }

    point** queries = NULL;
    uint32_t numQueries = 0;
    bool success = false;

    switch(ctx->source)
    {
        case KNN_RANDOM:
            logMessage("Generating random queries", INFO);
            numQueries = ctx->queryCount;
            point* randomPoints = generatePoints(numQueries, getMinCoord(), getMaxCoord());
            if(!randomPoints)
            {
                logMessage("Failed to generate random queries", ERROR);
                return 1;
            }

            queries = (point**)malloc(numQueries * sizeof(point*));
            for(uint32_t i = 0; i < numQueries; ++i)
                queries[i] = &randomPoints[i];
            break;

        case KNN_COORDINATES:
            logMessage("Using coordinate queries", INFO);
            queries = ctx->queries;
            numQueries = ctx->queryCount;
            break;

        default:
            logMessage("Unknown query source", ERROR);
            return 1;
    }

    if(!queries || numQueries == 0)
    {
        logMessage("No queries available", ERROR);
        return 1;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Executing %sKNN with k=%d on %u queries", ctx->approximate ? "approximate " : "", ctx->k, numQueries);
    logMessage(msg, INFO);

    KNNQueryBatch* batch = initKNNBatch(queries, numQueries, ctx->k);

    if(ctx->approximate)
        TIME_OP("Approximate KNN Batch", { batch = batchApproximateKNN(batch, ctx->epsilon); });
    else
        TIME_OP("Exact KNN Batch", { batch = batchKNN(batch); });

    if(!batch)
    {
        logMessage("KNN execution failed", ERROR);
        success = false;
    }
    else
    {
        success = true;

        printf("\n=== KNN Results ===\n");
        for(uint32_t i = 0; i < numQueries; ++i)
        {
            printf("\nQuery %u: (", i);
            for(uint8_t d = 0; d < getDimensions(); ++d)
            {
                printf("%.2f", queries[i]->coords[d]);
                if(d < getDimensions() - 1)
                    printf(", ");
            }

            printf(")\n");

            printf("Top-%d neighbors:\n", ctx->k);
            for(uint32_t j = 0; j < batch->results[i].count && j < ctx->k; ++j)
            {
                printf("  %u: (", j + 1);
                for(uint8_t d = 0; d < getDimensions(); ++d)
                {
                    printf("%.2f", batch->results[i].neighbors[j]->coords[d]);
                    if(d < getDimensions() - 1)
                        printf(", ");
                }

                printf(") - Distance: %.4f\n", batch->results[i].distances[j]);
            }
        }

        freeKNNBatch(batch);
    }

    if(ctx->source != KNN_COORDINATES && queries)
    {
        if(ctx->source == KNN_RANDOM && queries && queries[0])
        {
            free(queries[0]);
            free(queries);
        }
    }

    logMessage(success ? "KNN completed" : "KNN failed", INFO);
    return success ? 0 : 1;
}

int handleRange(void* context)
{
    logMessage("Starting RANGE query operation", DEBUG);

    RangeContext* ctx = (RangeContext*)context;
    if(!ctx)
    {
        logMessage("Invalid range context", ERROR);
        return 1;
    }

    KDTree* tree = getData()->tree;
    if(!tree)
    {
        logMessage("Tree not initialized. Use 'build' command first.", ERROR);
        return 1;
    }

    RangeBatch* batch = NULL;
    bool success = false;
    uint8_t dims = getDimensions();

    switch(ctx->source)
    {
        case RANGE_RANDOM:
        {
            logMessage("Generating random range queries", INFO);

            batch = initRangeBatch(ctx->queryCount);
            if(!batch)
            {
                logMessage("Failed to initialize range batch", ERROR);
                return 1;
            }

            float minCoord = getMinCoord();
            float maxCoord = getMaxCoord();
            float range = maxCoord - minCoord;

            for(uint32_t q = 0; q < ctx->queryCount; ++q)
            {
                for(uint8_t d = 0; d < dims; ++d)
                {
                    float center = minCoord + ((float)rand() / RAND_MAX) * range;
                    float halfSize = ((float)rand() / RAND_MAX) * (range * 0.2f);

                    batch->queries[q].minBounds[d] = center - halfSize;
                    batch->queries[q].maxBounds[d] = center + halfSize;
                }
            }

            break;
        }

        case RANGE_COORDINATES:
        {
            logMessage("Using coordinate range queries", INFO);

            batch = initRangeBatch(ctx->queryCount);
            if(!batch)
            {
                logMessage("Failed to initialize range batch", ERROR);
                return 1;
            }

            for(uint32_t q = 0; q < ctx->queryCount; ++q)
            {
                for(uint8_t d = 0; d < dims; ++d)
                {
                    batch->queries[q].minBounds[d] = ctx->queries[q][d];
                    batch->queries[q].maxBounds[d] = ctx->queries[q][d + dims];
                }
            }

            break;
        }

        default:
            logMessage("Unknown range source", ERROR);
            return 1;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Executing range queries on %u queries", ctx->queryCount);
    logMessage(msg, INFO);

    TIME_OP("Batch Range Query", { success = batchRangeQuery(batch); });

    if(!success)
    {
        logMessage("Range query execution failed", ERROR);
        freeRangeBatch(batch);
        return 1;
    }

    printf("\n=== Range Query Results ===\n");
    size_t totalPoints = 0;

    for(uint32_t q = 0; q < ctx->queryCount; ++q)
    {
        printf("\nQuery %u: [", q);
        for(uint8_t d = 0; d < dims; ++d)
        {
            printf("%.2f, %.2f", batch->queries[q].minBounds[d], batch->queries[q].maxBounds[d]);
            if(d < dims - 1)
                printf("] x [");
        }

        printf("]\n");

        printf("Found %zu points:\n", batch->results[q].count);

        size_t displayCount = batch->results[q].count;
        if(displayCount > 10)
        {
            printf("  (Showing first 10 of %zu)\n", displayCount);
            displayCount = 10;
        }

        for(size_t p = 0; p < displayCount; ++p)
        {
            printf("  %zu: (", p + 1);
            for(uint8_t d = 0; d < dims; ++d)
            {
                printf("%.2f", batch->results[q].points[p]->coords[d]);
                if(d < dims - 1) printf(", ");
            }

            printf(")\n");
        }

        totalPoints += batch->results[q].count;
    }

    printf("\nTotal points across all queries: %zu\n", totalPoints);

    freeRangeBatch(batch);

    logMessage(success ? "Range query completed" : "Range query failed", INFO);
    return success ? 0 : 1;
}

int handleClusterDPC(void* context)
{
    logMessage("Starting Density Peak Clustering", DEBUG);

    DPCContext* ctx = (DPCContext*)context;
    if(!ctx)
    {
        logMessage("Invalid DPC context", ERROR);
        return 1;
    }

    KDTree* tree = getData()->tree;
    if(!tree)
    {
        logMessage("Tree not initialized. Use 'build' command first.", ERROR);
        return 1;
    }

    point** allPoints = NULL;
    size_t totalPoints = 0;
    size_t capacity = 0;

    collectAllPoints(tree->root, &allPoints, &totalPoints, &capacity);

    if(totalPoints == 0 || !allPoints)
    {
        logMessage("No points found in tree", ERROR);
        return 1;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Running DPC on %zu points with k=%u, density_thresh=%.2f, delta_thresh=%.2f", totalPoints, ctx->kNeighbors, ctx->minDensityThreshold, ctx->minDeltaThreshold);
    logMessage(msg, INFO);

    DPCConfig config = {
        .minDensityThreshold = ctx->minDensityThreshold,
        .minDeltaThreshold = ctx->minDeltaThreshold,
        .kNeighbors = ctx->kNeighbors
    };

    DPCResult* result = NULL;

    TIME_OP("Density Peak Clustering", { result = densityPeakClustering(allPoints, (uint32_t)totalPoints, &config); });

    if(!result)
    {
        logMessage("DPC execution failed", ERROR);
        free(allPoints);
        return 1;
    }

    printf("\n=== Density Peak Clustering Results ===\n");
    printf("Total points: %u\n", result->numPoints);
    printf("Number of clusters detected: %u\n", result->numCenters);

    uint32_t* clusterSizes = (uint32_t*)calloc(result->numCenters, sizeof(uint32_t));
    for(uint32_t i = 0; i < result->numPoints; ++i)
        if(result->clusterAssignment[i] < result->numCenters)
            ++clusterSizes[result->clusterAssignment[i]];

    printf("\nCluster distribution:\n");
    for(uint32_t c = 0; c < result->numCenters; ++c)
    {
        float percentage = (float)clusterSizes[c] / result->numPoints * 100.0f;
        printf("  Cluster %u: %u points (%.1f%%)\n", c, clusterSizes[c], percentage);
    }

    printf("\nCluster centers (points with highest density):\n");
    for(uint32_t c = 0; c < result->numCenters; ++c)
    {
        uint32_t centerIdx = result->clusterCenters[c];
        printf("  Cluster %u center: (", c);
        for(uint8_t d = 0; d < getDimensions(); ++d)
        {
            printf("%.2f", allPoints[centerIdx]->coords[d]);
            if(d < getDimensions() - 1)
                printf(", ");
        }

        printf(") - Density: %.4f\n", result->densities[centerIdx]);
    }

    free(allPoints);
    free(clusterSizes);
    freeDPCResult(result);

    logMessage("DPC completed successfully", INFO);
    return 0;
}

int handleClusterDBSCAN(void* context)
{
    logMessage("Starting DBSCAN clustering", DEBUG);

    DBSCANContext* ctx = (DBSCANContext*)context;
    if(!ctx)
    {
        logMessage("Invalid DBSCAN context", ERROR);
        return 1;
    }

    KDTree* tree = getData()->tree;
    if(!tree)
    {
        logMessage("Tree not initialized. Use 'build' command first.", ERROR);
        return 1;
    }

    point** allPoints = NULL;
    size_t totalPoints = 0;
    size_t capacity = 0;

    collectAllPoints(tree->root, &allPoints, &totalPoints, &capacity);

    if(totalPoints == 0 || !allPoints)
    {
        logMessage("No points found in tree", ERROR);
        return 1;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Running DBSCAN on %zu points with epsilon=%.4f, minPts=%u, gridSize=%.4f", totalPoints, ctx->epsilon, ctx->minPts, ctx->gridCellSize);
    logMessage(msg, INFO);

    DBSCANConfig config = {
        .epsilon = ctx->epsilon,
        .minPts = ctx->minPts,
        .gridCellSize = ctx->gridCellSize
    };

    DBSCANResult* result = NULL;

    TIME_OP("DBSCAN Clustering", { result = dbscan(allPoints, (uint32_t)totalPoints, &config); });

    if(!result)
    {
        logMessage("DBSCAN execution failed", ERROR);
        free(allPoints);
        return 1;
    }

    printf("\n=== DBSCAN Clustering Results ===\n");
    printf("Total points: %u\n", result->numPoints);
    printf("Number of clusters: %u\n", result->numClusters);
    printf("Noise points: %u\n", result->numNoise);

    if(result->numClusters > 0)
    {
        uint32_t* clusterSizes = (uint32_t*)calloc(result->numClusters, sizeof(uint32_t));

        for(uint32_t i = 0; i < result->numPoints; ++i)
        {
            if(result->labels[i] != UINT32_MAX)
                clusterSizes[result->labels[i]]++;
        }

        printf("\nCluster distribution:\n");
        for(uint32_t c = 0; c < result->numClusters; ++c)
        {
            float percentage = (float)clusterSizes[c] / result->numPoints * 100.0f;
            printf("  Cluster %u: %u points (%.1f%%)\n", c, clusterSizes[c], percentage);
        }

        free(clusterSizes);
    }

    if(result->numClusters > 0 && result->numClusters <= 10)
    {
        printf("\nSample points per cluster:\n");

        for(uint32_t c = 0; c < result->numClusters; ++c)
        {
            printf("  Cluster %u: ", c);
            uint32_t sampleCount = 0;

            for(uint32_t i = 0; i < result->numPoints && sampleCount < 3; ++i)
            {
                if(result->labels[i] == c)
                {
                    printf("(");
                    for(uint8_t d = 0; d < getDimensions(); ++d)
                    {
                        printf("%.2f", allPoints[i]->coords[d]);
                        if(d < getDimensions() - 1)
                            printf(", ");
                    }

                    printf(") ");
                    ++sampleCount;
                }
            }

            printf("\n");
        }
    }

    if(result->numNoise > 0)
        printf("\nNoise points: %u points not assigned to any cluster\n", result->numNoise);

    freeDBSCANResult(result);
    free(allPoints);

    logMessage("DBSCAN completed successfully", INFO);
    return 0;
}

int handleBenchmark(void* context)
{
    return 0;
}

int handleInfo(void* context)
{
    logMessage("Printing tree information", DEBUG);

    PrintOptions* ctx = (PrintOptions*)context;
    if(!ctx)
    {
        logMessage("Invalid print context", ERROR);
        return 1;
    }

    if(!getData() || !getData()->tree)
    {
        logMessage("Tree not available", ERROR);
        return 1;
    }

    TIME_OP("Print KD-Tree Info", { printKDTree(getData()->tree->root, ctx); });

    logMessage("Tree information printed", INFO);
    return 0;
}

int handleConfig(void* context)
{
    logMessage("Processing configuration command", DEBUG);

    if(!context)
    {
        logMessage("Invalid configuration context", ERROR);
        return 1;
    }

    ConfigContext ctx = *(ConfigContext*)context;

    switch(ctx.type)
    {
        case INIT:
            logMessage("Initializing configuration", INFO);
            TIME_OP("Init Config", { initConfig(); });
            return 0;

        case RESET:
            logMessage("Resetting configuration", INFO);
            TIME_OP("Reset Config", { resetConfig(); });
            return 0;

        case CONFIGURATION:
            logMessage("Printing configuration", INFO);
            TIME_OP("Print Configuration", { printConfig(); });
            return 0;

        case SPECIFICS:
            logMessage("Printing system metrics", INFO);
            TIME_OP("Print System Metrics", { printSystemMetrics(); });
            return 0;

        case DATASET:
            logMessage("Printing dataset", INFO);
            TIME_OP("Print Dataset", { printDataset(readDataset(ctx.dataset), getNPoint()); });
            return 0;

        default:
            logMessage("Unknown configuration command", ERROR);
            return 1;
    }
}

int handleSet(void* context)
{
    logMessage("Updating configuration parameter", DEBUG);

    if(!context || !((SetContext*)context)->value)
    {
        logMessage("Invalid set context or value", ERROR);
        return 1;
    }

    SetContext* setCtx = (SetContext*)context;

    switch(setCtx->type)
    {
        case NPOINT:
            logMessage("Setting number of points", INFO);
            TIME_OP("Set Number of Points", { setNPoint(*(uint32_t*)setCtx->value); });
            break;

        case MINCOORD:
            logMessage("Setting minimum coordinate", INFO);
            TIME_OP("Set Min Coordinate Value", { setMinCoord(*(double*)setCtx->value); });
            break;

        case MAXCOORD:
            logMessage("Setting maximum coordinate", INFO);
            TIME_OP("Set Max Coordinate Value", { setMaxCoord(*(double*)setCtx->value); });
            break;

        case NPIM:
            logMessage("Setting PIM value", INFO);
            TIME_OP("Set PIM Value", { setNPim(*(uint8_t*)setCtx->value); });
            break;

        case DIMENSIONS:
            logMessage("Setting dimensions", INFO);
            TIME_OP("Set Dimensions", { setDimensions(*(uint8_t*)setCtx->value); });
            break;

        case ALPHA:
            logMessage("Setting alpha", INFO);
            TIME_OP("Set Alpha", { setAlpha(*(float*)setCtx->value); });
            break;

        case BETA:
            logMessage("Setting beta", INFO);
            TIME_OP("Set Beta", { setBeta(*(float*)setCtx->value); });
            break;

        case LEAFWRAPTHRESHOLD:
            logMessage("Setting leaf wrap threshold", INFO);
            TIME_OP("Set Leaf Wrap Threshold", { setLeafWrapThreshold(*(uint16_t*)setCtx->value); });
            break;

        case OVERSAMPLINGRATE:
            logMessage("Setting oversampling rate", INFO);
            TIME_OP("Set Oversampling Rate", { setOversamplingRate(*(uint16_t*)setCtx->value); });
            break;

        case SKETCHHEIGHT:
            logMessage("Setting sketch height", INFO);
            TIME_OP("Set Sketch Height", { setSketchHeight(*(uint8_t*)setCtx->value); });
            break;

        case CHUNKSIZE:
            logMessage("Setting chunk size", INFO);
            TIME_OP("Set Chunk Size", { setChunkSize(*(uint16_t*)setCtx->value); });
            break;

        default:
            logMessage("Unknown parameter type", ERROR);
            return 1;
    }

    free(setCtx->value);
    free(setCtx);

    logMessage("Configuration updated", INFO);
    return 0;
}

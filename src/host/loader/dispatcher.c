#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "host/kdTree/serialization.h"
#include "host/kdTree/free.h"

#include "host/loader/dispatcher.h"
#include "host/loader/config.h"

#include "host/environment/init.h"
#include "host/environment/constants.h"

#include "host/management/logging.h"

static uint32_t getMaxCount(uint32_t* counts, uint32_t numDpus)
{
    uint32_t max = 0;
    for(size_t i = 0; i < numDpus; ++i)
        if(counts[i] > max)
            max = counts[i];

    return max;
}

static size_t getMaxTotalSize(size_t* sizes, uint32_t numDpus)
{
    size_t max = 0;
    for(uint32_t i = 0; i < numDpus; ++i)
        if(sizes[i] > max)
            max = sizes[i];

    return max;
}

int dispatchBuildTask(point*** perPimPoints, size_t* perPimCounts, uint32_t numDpus, uint8_t*** allOutputBuffers, size_t** allOutputSizes, uint64_t*** allNodeMaps, size_t** allMapSizes)
{
    struct dpu_set_t set;
    struct dpu_set_t dpu;
    uint32_t dpuIdx = 0;
    char msgBuf[256];

    DPU_ASSERT(dpu_alloc(numDpus, NULL, &set));

    *allOutputBuffers = (uint8_t**)calloc(numDpus, sizeof(uint8_t*));
    *allOutputSizes = (size_t*)calloc(numDpus, sizeof(size_t));
    *allNodeMaps = (uint64_t**)calloc(numDpus, sizeof(uint64_t*));
    *allMapSizes = (size_t*)calloc(numDpus, sizeof(size_t));

    if(!*allOutputBuffers || !*allOutputSizes || !*allNodeMaps || !*allMapSizes)
    {
        logMessage("Failed to allocate host result buffers", ERROR);
        return -1;
    }

    logMessage("Loading program Build.dpu on all DPUs", DEBUG);
    DPU_ASSERT(dpu_load(set, "build/Build.dpu", NULL));
    sendConfigToDpu(set);

    logMessage("Sending data to DPUs using MRAM symbols", DEBUG);

    DPU_FOREACH(set, dpu)
    {
        if(perPimCounts[dpuIdx] > 0)
        {
            uint32_t dims = getDimensions();
            size_t pointsSize = (size_t)perPimCounts[dpuIdx] * dims * sizeof(float);
            size_t alignedPointsSize = (pointsSize + 7) & ~7;

            float* linearPoints = (float*)calloc(1, alignedPointsSize);
            for(size_t i = 0; i < perPimCounts[dpuIdx]; ++i)
                memcpy(linearPoints + i * dims, perPimPoints[dpuIdx][i]->coords, dims * sizeof(float));

            DPU_ASSERT(dpu_prepare_xfer(dpu, linearPoints));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "MRAM_INPUT_BUFFER", 0, alignedPointsSize, DPU_XFER_DEFAULT));

            DPU_ASSERT(dpu_copy_to(dpu, "nPoints", 0, &perPimCounts[dpuIdx], sizeof(uint32_t)));

            snprintf(msgBuf, sizeof(msgBuf), "DPU %u: Sent %zu points to MRAM_INPUT_BUFFER", dpuIdx, perPimCounts[dpuIdx]);
            logMessage(msgBuf, DEBUG);

            free(linearPoints);
        }

        ++dpuIdx;
    }

    logMessage("Launching DPUs...", INFO);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    if(DEBUG <= getVerbosity())
    {
        DPU_FOREACH(set, dpu)
        {
            DPU_ASSERT(dpu_log_read(dpu, stdout));
            break;
        }
    }

    logMessage("Collecting results from MRAM output symbols", DEBUG);
    dpuIdx = 0;
    DPU_FOREACH(set, dpu)
    {
        if(perPimCounts[dpuIdx] > 0)
        {
            uint32_t outSizeTemp = 0;
            uint32_t mapSizeTemp = 0;

            DPU_ASSERT(dpu_copy_from(dpu, "outputSize", 0, &outSizeTemp, sizeof(uint32_t)));
            DPU_ASSERT(dpu_copy_from(dpu, "mapSize", 0, &mapSizeTemp, sizeof(uint32_t)));

            (*allOutputSizes)[dpuIdx] = (size_t)outSizeTemp;
            (*allMapSizes)[dpuIdx] = (size_t)mapSizeTemp;

            if(outSizeTemp > 0)
            {
                size_t alignedOutSize = (outSizeTemp + 7) & ~7;
                (*allOutputBuffers)[dpuIdx] = (uint8_t*)malloc(alignedOutSize);

                DPU_ASSERT(dpu_prepare_xfer(dpu, (*allOutputBuffers)[dpuIdx]));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "MRAM_OUTPUT_BUFFER", 0, alignedOutSize, DPU_XFER_DEFAULT));
            }

            if(mapSizeTemp > 0)
            {
                size_t mapByteSize = (size_t)mapSizeTemp * sizeof(uint64_t);
                size_t alignedMapSize = (mapByteSize + 7) & ~7;
                (*allNodeMaps)[dpuIdx] = (uint64_t*)malloc(alignedMapSize);

                DPU_ASSERT(dpu_prepare_xfer(dpu, (*allNodeMaps)[dpuIdx]));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "MRAM_NODE_MAP", 0, alignedMapSize, DPU_XFER_DEFAULT));
            }

            snprintf(msgBuf, sizeof(msgBuf), "DPU %u: Collected Tree (%u bytes) and Map (%u entries)", dpuIdx, outSizeTemp, mapSizeTemp);
            logMessage(msgBuf, DEBUG);
        }

        ++dpuIdx;
    }

    DPU_ASSERT(dpu_free(set));
    return 0;
}

int dispatchDistributeReplicas(ReplicaDistribution** perDpuDistributions, uint32_t* perDpuCounts, uint8_t*** perDpuBuffers, size_t** perDpuBufferSizes, uint32_t numDpus, uint8_t* sketchData, size_t sketchSize, DpuAllocation* alloc)
{
    char msgBuf[256];
    struct dpu_set_t set, dpu;
    uint32_t dpuId;

    logMessage("Starting global distribution with DMA alignment", INFO);

    DPU_ASSERT(dpu_alloc(numDpus, NULL, &set));
    DPU_ASSERT(dpu_load(set, "build/Distribute.dpu", NULL));

    uint8_t** linearBuffers = (uint8_t**)malloc(numDpus * sizeof(uint8_t*));
    size_t* alignedTotalSizes = (size_t*)malloc(numDpus * sizeof(size_t));

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        size_t dpuSizeWithPadding = 0;
        for(uint32_t i = 0; i < perDpuCounts[dpuId]; ++i)
            dpuSizeWithPadding += (perDpuBufferSizes[dpuId][i] + 7) & ~7;

        alignedTotalSizes[dpuId] = dpuSizeWithPadding;

        if(dpuSizeWithPadding > 0)
        {
            linearBuffers[dpuId] = (uint8_t*)calloc(1, dpuSizeWithPadding);
            size_t offset = 0;
            for(uint32_t i = 0; i < perDpuCounts[dpuId]; ++i)
            {
                memcpy(linearBuffers[dpuId] + offset, perDpuBuffers[dpuId][i], perDpuBufferSizes[dpuId][i]);
                offset += (perDpuBufferSizes[dpuId][i] + 7) & ~7;
            }
        }
        else
            linearBuffers[dpuId] = NULL;
        ++dpuId;
    }

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &perDpuCounts[dpuId]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "pendingCount", 0, sizeof(uint32_t), DPU_XFER_DEFAULT));

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        if(perDpuCounts[dpuId] > 0)
            DPU_ASSERT(dpu_prepare_xfer(dpu, perDpuDistributions[dpuId]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "distributions", 0, sizeof(ReplicaDistribution) * getMaxCount(perDpuCounts, numDpus), DPU_XFER_DEFAULT));

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        if(alignedTotalSizes[dpuId] > 0)
            DPU_ASSERT(dpu_prepare_xfer(dpu, linearBuffers[dpuId]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "allReplicasBuffer", 0, getMaxTotalSize(alignedTotalSizes, numDpus), DPU_XFER_DEFAULT));

    if(sketchData && sketchSize > 0)
    {
        size_t alignedSketchSize = (sketchSize + 7) & ~7;
        DPU_ASSERT(dpu_broadcast_to(set, "sketchData", 0, sketchData, alignedSketchSize, DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_broadcast_to(set, "sketchSize", 0, &sketchSize, sizeof(size_t), DPU_XFER_DEFAULT));
    }

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        uint8_t status;
        DPU_ASSERT(dpu_copy_from(dpu, "finalStatus", 0, &status, sizeof(uint8_t)));
        if(status != 0)
        {
            snprintf(msgBuf, sizeof(msgBuf), "DPU %u failed with status %u", dpuId, status);
            logMessage(msgBuf, ERROR);
        }

        ++dpuId;
    }

    for(uint32_t i = 0; i < numDpus; ++i)
        if(linearBuffers[i])
            free(linearBuffers[i]);

    free(linearBuffers);
    free(alignedTotalSizes);
    dpu_free(set);
    return 0;
}

int dispatchGroup0Search(KDNode* root, float*** queriesPerDpu, uint32_t* queriesCounts, uint32_t numDpus, uint32_t dimensions, KDNode*** resultsPerDpu)
{
    struct dpu_set_t set, dpu;
    uint32_t dpuId = 0;

    size_t queryByteSize = dimensions * sizeof(float);
    size_t alignedQuerySize = (queryByteSize + 7) & ~7;

    DPU_ASSERT(dpu_alloc(numDpus, NULL, &set));
    DPU_ASSERT(dpu_load(set, "build/Search0.dpu", NULL));

    float** linearQueriesBuffers = (float**)malloc(numDpus * sizeof(float*));
    size_t maxQueriesSize = 0;

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        uint32_t count = queriesCounts[dpuId];
        uint64_t args[3] = { (uint64_t)(uintptr_t)root, count, dimensions };

        DPU_ASSERT(dpu_prepare_xfer(dpu, args));

        if(count > 0)
        {
            size_t totalDpuQuerySize = count * alignedQuerySize;
            linearQueriesBuffers[dpuId] = (float*)calloc(1, totalDpuQuerySize);

            for(uint32_t i = 0; i < count; ++i)
            {
                uint8_t* target = ((uint8_t*)linearQueriesBuffers[dpuId]) + (i * alignedQuerySize);
                memcpy(target, queriesPerDpu[dpuId][i], queryByteSize);
            }

            if(totalDpuQuerySize > maxQueriesSize)
                maxQueriesSize = totalDpuQuerySize;

            DPU_ASSERT(dpu_prepare_xfer(dpu, linearQueriesBuffers[dpuId]));
        }
        else
            linearQueriesBuffers[dpuId] = NULL;
        ++dpuId;
    }

    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "group0Args", 0, sizeof(uint64_t)*3, DPU_XFER_DEFAULT));
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "group0Queries", 0, maxQueriesSize, DPU_XFER_DEFAULT));

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    *resultsPerDpu = (KDNode**)malloc(numDpus * sizeof(KDNode*));
    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        uint32_t count = queriesCounts[dpuId];
        if(count > 0)
        {
            size_t resSize;
            DPU_ASSERT(dpu_copy_from(dpu, "group0ResultsSize", 0, &resSize, sizeof(size_t)));

            uint64_t* rawResults = (uint64_t*)malloc(resSize);
            DPU_ASSERT(dpu_copy_from(dpu, "group0Results", 0, rawResults, resSize));

            KDNode** resultsArray = (KDNode**)malloc(count * sizeof(KDNode*));
            for(uint32_t i = 0; i < count; ++i)
                resultsArray[i] = (KDNode*)(uintptr_t)rawResults[i];

            (*resultsPerDpu)[dpuId] = (KDNode*)resultsArray;
            free(rawResults);
        }
        else
            (*resultsPerDpu)[dpuId] = NULL;

        if(linearQueriesBuffers[dpuId])
            free(linearQueriesBuffers[dpuId]);

        ++dpuId;
    }

    free(linearQueriesBuffers);
    DPU_ASSERT(dpu_free(set));
    return 0;
}

int dispatchPullNode(KDNode* node, uint32_t numQueries, float** queries, uint32_t dimensions, KDNode*** resultsPerQuery)
{
    char msgBuf[256];
    struct dpu_set_t set;

    if(!node || !queries || numQueries == 0)
    {
        logMessage("Invalid parameters for Pull Node Task", ERROR);
        return -1;
    }

    size_t queryByteSize = dimensions * sizeof(float);
    size_t alignedQuerySize = (queryByteSize + 7) & ~7;
    size_t totalQueriesSize = (size_t)numQueries * alignedQuerySize;

    snprintf(msgBuf, sizeof(msgBuf), "Pull Node: Queries=%u, Dims=%u, AlignedQuerySize=%zu", numQueries, dimensions, alignedQuerySize);
    logMessage(msgBuf, INFO);

    DPU_ASSERT(dpu_alloc(1, NULL, &set));
    DPU_ASSERT(dpu_load(set, "build/Pull.dpu", NULL));
    sendConfigToDpu(set);

    uint64_t args[3] = { (uint64_t)(uintptr_t)node, (uint64_t)numQueries, (uint64_t)dimensions };

    DPU_ASSERT(dpu_copy_to(set, "pullArgs", 0, args, sizeof(args)));

    float* linearQueries = (float*)calloc(1, totalQueriesSize);
    if(!linearQueries)
    {
        logMessage("Failed to allocate linearQueries (Host)", ERROR);
        dpu_free(set);
        return -1;
    }

    for(uint32_t i = 0; i < numQueries; ++i)
    {
        uint8_t* dest = ((uint8_t*)linearQueries) + (i * alignedQuerySize);
        memcpy(dest, queries[i], queryByteSize);
    }

    DPU_ASSERT(dpu_copy_to(set, "pullQueries", 0, linearQueries, totalQueriesSize));
    free(linearQueries);

    logMessage("Launching pull node task on DPU", DEBUG);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    *resultsPerQuery = (KDNode**)calloc(numQueries, sizeof(KDNode*));

    size_t sSize = 0, dResSize = 0;
    DPU_ASSERT(dpu_copy_from(set, "subtreeSize", 0, &sSize, sizeof(size_t)));
    DPU_ASSERT(dpu_copy_from(set, "directResultsSize", 0, &dResSize, sizeof(size_t)));

    if(sSize > 0)
    {
        logMessage("Subtree Pull", INFO);

        size_t alignedSubtreeSize = (sSize + 7) & ~7;
        uint8_t* sBuffer = (uint8_t*)malloc(alignedSubtreeSize);

        if(sBuffer)
        {
            DPU_ASSERT(dpu_copy_from(set, "subtreeData", 0, sBuffer, alignedSubtreeSize));

            KDNode* pulledSubtree = deserializeTree(sBuffer, sSize);
            if(pulledSubtree) {
                for(uint32_t i = 0; i < numQueries; ++i)
                {
                    KDNode* current = pulledSubtree;
                    while(current && current->type == INTERNAL)
                    {
                        uint8_t d = current->data.internal.splitDim;
                        current = (queries[i][d] < current->data.internal.splitValue) ? current->data.internal.left : current->data.internal.right;
                    }
                    (*resultsPerQuery)[i] = current;
                }
            }

            free(sBuffer);
        }
    }
    else if(dResSize > 0)
    {
        logMessage("Direct DPU Search", INFO);

        uint64_t* rawRes = (uint64_t*)malloc(dResSize);
        if(rawRes)
        {
            DPU_ASSERT(dpu_copy_from(set, "directResults", 0, rawRes, dResSize));

            for(uint32_t i = 0; i < numQueries; ++i)
                (*resultsPerQuery)[i] = (KDNode*)(uintptr_t)rawRes[i];

            free(rawRes);
        }
    }

    DPU_ASSERT(dpu_free(set));
    logMessage("Pull node task completed successfully", DEBUG);
    return 0;
}

int dispatchPushSearch(KDNode* node, uint32_t numQueries, float** queries, uint32_t dimensions, uint8_t groupId, KDNode** results)
{
    char msgBuf[256];
    struct dpu_set_t set;

    if(!node || !queries || numQueries == 0 || !results)
    {
        logMessage("Invalid parameters for Push Search", ERROR);
        return -1;
    }

    size_t queryByteSize = dimensions * sizeof(float);
    size_t alignedQuerySize = (queryByteSize + 7) & ~7;
    size_t totalQueriesSize = (size_t)numQueries * alignedQuerySize;

    snprintf(msgBuf, sizeof(msgBuf), "Push Search: Q=%u, D=%u, GID=%u, AlignedSize=%zu", numQueries, dimensions, groupId, alignedQuerySize);
    logMessage(msgBuf, INFO);

    DPU_ASSERT(dpu_alloc(1, NULL, &set));
    DPU_ASSERT(dpu_load(set, "build/Push.dpu", NULL));
    sendConfigToDpu(set);

    uint64_t args[4] = { (uint64_t)(uintptr_t)node, (uint64_t)numQueries, (uint64_t)dimensions, (uint64_t)groupId };

    DPU_ASSERT(dpu_copy_to(set, "pushArgs", 0, args, sizeof(args)));

    float* linearQueries = (float*)calloc(1, totalQueriesSize);
    if(!linearQueries)
    {
        logMessage("Failed to allocate linearQueries (Host)", ERROR);
        dpu_free(set);
        return -1;
    }

    for(uint32_t i = 0; i < numQueries; ++i)
    {
        uint8_t* dest = ((uint8_t*)linearQueries) + (i * alignedQuerySize);
        memcpy(dest, queries[i], queryByteSize);
    }

    DPU_ASSERT(dpu_copy_to(set, "pushQueries", 0, linearQueries, totalQueriesSize));
    free(linearQueries);

    logMessage("Launching Push search execution", DEBUG);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    size_t resByteSize;
    DPU_ASSERT(dpu_copy_from(set, "pushResultsSize", 0, &resByteSize, sizeof(size_t)));

    if(resByteSize > 0)
    {
        uint64_t* rawResults = (uint64_t*)malloc(resByteSize);
        if(rawResults)
        {
            DPU_ASSERT(dpu_copy_from(set, "pushResults", 0, rawResults, resByteSize));

            uint32_t count = resByteSize / sizeof(uint64_t);
            for(uint32_t i = 0; i < count && i < numQueries; ++i)
                results[i] = (KDNode*)(uintptr_t)rawResults[i];

            free(rawResults);
        }
    }

    DPU_ASSERT(dpu_free(set));
    logMessage("Push search completed successfully", DEBUG);
    return 0;
}

int dispatchBatchUpdate(DpuBatchOperation* operations, uint32_t count, DpuResult*** resultsPerDpu, uint32_t** resultCountsPerDpu, uint32_t numDpus)
{
    struct dpu_set_t set, dpu;
    uint32_t dpuId = 0;

    if(!operations || count == 0 || !resultsPerDpu || !resultCountsPerDpu)
    {
        logMessage("Invalid parameters for Batch Update", ERROR);
        return -1;
    }

    DPU_ASSERT(dpu_alloc(numDpus, NULL, &set));
    DPU_ASSERT(dpu_load(set, "build/Update.dpu", NULL));
    sendConfigToDpu(set);

    *resultsPerDpu = (DpuResult**)calloc(numDpus, sizeof(DpuResult*));
    *resultCountsPerDpu = (uint32_t*)calloc(numDpus, sizeof(uint32_t));

    DpuBatchOperation** opsPerDpu = (DpuBatchOperation**)calloc(numDpus, sizeof(DpuBatchOperation*));
    uint32_t* opsCountPerDpu = (uint32_t*)calloc(numDpus, sizeof(uint32_t));
    uint32_t* opsCapacityPerDpu = (uint32_t*)calloc(numDpus, sizeof(uint32_t));

    for(uint32_t i = 0; i < count; ++i)
    {
        uint32_t id = (uint32_t)(operations[i].targetNodeAddr & 0xFF) % numDpus;
        if(opsCountPerDpu[id] >= opsCapacityPerDpu[id])
        {
            uint32_t newCap = opsCapacityPerDpu[id] ? opsCapacityPerDpu[id] * 2 : 16;
            opsPerDpu[id] = (DpuBatchOperation*)realloc(opsPerDpu[id], newCap * sizeof(DpuBatchOperation));
            opsCapacityPerDpu[id] = newCap;
        }

        opsPerDpu[id][opsCountPerDpu[id]++] = operations[i];
    }

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &opsCountPerDpu[dpuId]));
    }

    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "batchCount", 0, sizeof(uint32_t), DPU_XFER_DEFAULT));

    size_t maxAlignedOpsSize = 0;
    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        if(opsCountPerDpu[dpuId] > 0)
        {
            size_t rawSize = opsCountPerDpu[dpuId] * sizeof(DpuBatchOperation);
            size_t alignedSize = (rawSize + 7) & ~7;
            if(alignedSize > maxAlignedOpsSize)
                maxAlignedOpsSize = alignedSize;

            DPU_ASSERT(dpu_prepare_xfer(dpu, opsPerDpu[dpuId]));
        }
    }

    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "batchOperations", 0, maxAlignedOpsSize, DPU_XFER_DEFAULT));

    logMessage("Launching parallel Batch Update", DEBUG);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    dpuId = 0;
    DPU_FOREACH(set, dpu)
    {
        uint32_t localResCount;
        DPU_ASSERT(dpu_copy_from(dpu, "resultCount", 0, &localResCount, sizeof(uint32_t)));

        if(localResCount > 0)
        {
            (*resultCountsPerDpu)[dpuId] = localResCount;
            size_t rawResSize = localResCount * sizeof(DpuResult);
            size_t alignedResSize = (rawResSize + 7) & ~7;

            (*resultsPerDpu)[dpuId] = (DpuResult*)malloc(alignedResSize);
            DPU_ASSERT(dpu_copy_from(dpu, "batchResults", 0, (*resultsPerDpu)[dpuId], alignedResSize));
        }

        if(opsPerDpu[dpuId])
            free(opsPerDpu[dpuId]);

        ++dpuId;
    }

    free(opsPerDpu);
    free(opsCountPerDpu);
    free(opsCapacityPerDpu);
    DPU_ASSERT(dpu_free(set));

    logMessage("Parallel Batch Update completed", INFO);
    return 0;
}

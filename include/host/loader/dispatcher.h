#ifndef HOST_LOADER_TASK_DISPATCHER_H
#define HOST_LOADER_TASK_DISPATCHER_H

#include <stdint.h>
#include <dpu.h>

#include "host/kdTree/types.h"
#include "host/kdTree/update.h"

typedef enum TaskType
{
    BUILD_ONCHIP,
    DISTRIBUTE_REPLICAS,
    GROUP0_SEARCH,
    PULL_NODE,
    PUSH_SEARCH,
    BATCH_UPDATE
} TaskType;

typedef struct TaskHeader
{
    TaskType type;
} TaskHeader;

typedef struct ReplicaDistribution
{
    uint8_t groupId;
    uint32_t replicaIndex;
    uint64_t replicaAddress;
    uint64_t masterAddress;
    size_t replicaSize;
} ReplicaDistribution;

int dispatchBuildTask(point*** perPimPoints, size_t* perPimCounts, uint32_t numDpus, uint8_t*** allOutputBuffers, size_t** allOutputSizes, uint64_t*** allNodeMaps, size_t** allMapSizes);

int dispatchDistributeReplicas(ReplicaDistribution** perDpuDistributions,uint32_t* perDpuCounts, uint8_t*** perDpuBuffers, size_t** perDpuBufferSizes, uint32_t numDpus, uint8_t* sketchData, size_t sketchSize, DpuAllocation* alloc);

int dispatchGroup0Search(KDNode* root, float*** queriesPerDpu, uint32_t* queriesCounts, uint32_t numDpus, uint32_t dimensions, KDNode*** resultsPerDpu);

int dispatchPullNode(KDNode* node, uint32_t numQueries, float** queries, uint32_t dimensions, KDNode*** resultsPerQuery);

int dispatchPushSearch(KDNode* node, uint32_t numQueries, float** queries, uint32_t dimensions, uint8_t groupId, KDNode** results);

int dispatchBatchUpdate(DpuBatchOperation* operations, uint32_t count, DpuResult*** resultsPerDpu, uint32_t** resultCountsPerDpu, uint32_t numDpus);

#endif

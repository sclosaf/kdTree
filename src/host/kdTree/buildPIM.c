#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <omp.h>

#include "management/dpuManagement.h"
#include "kdTree/build.h"
#include "utils/constants.h"
#include "utils/types.h"

typedef struct ReplicaInfo
{
    uint32_t dpuId;
    uint32_t nodeOffset;
    uint32_t nodeCount;
    uint8_t groupLevel;
} ReplicaInfo;

typedef struct DpuAllocation
{
    uint32_t* nextOffset;
    uint32_t* allocationCount;
    size_t numDpus;
} DpuAllocation;

static void traverseSketchAndAssign(KDNode* sketch, point** points, size_t n, size_t P, point*** perPimPoints, size_t* perPimCounts);
static void sendSketchToAllDpus(DPUContext* ctx, KDNode* sketch);
static void serializeNodeData(KDNode* node, uint8_t** ptr);
static void serializeNodeSize(KDNode* node, size_t* size);
static void* serializeTree(KDNode* root, size_t* size);
static void* serializeNode(KDNode* node, size_t* size);
static KDNode** collectSubtreesFromDpus(DPUContext* dpuCtx, size_t P, size_t* totalNodes);
static void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity);
static void assignNodesToGroups(KDNode* node, size_t totalPoints, uint8_t* groupLevels);
static void scatterReplicas(DPUContext* dpuCtx, KDNode** subtrees, size_t P, size_t totalPoints, KDNode* cacheForest);
static KDNode* deserializeTree(void* data, size_t size);
static KDNode* deserializeNode(uint8_t** ptr, uint8_t* end);

static DpuAllocation* createDpuAllocation(size_t numDpus);
static void freeDpuAllocation(DpuAllocation* alloc);
static uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size);


KDTree* buildPIMKDTree(point** points, size_t n, DPUContext* dpuCtx)
{
    if(!points || n == 0 || !dpuCtx)
        return NULL;

    size_t P = dpuCtx->nDpus;

    DpuAllocation* alloc = createDpuAllocation(P);
    if(!alloc)
        return NULL;

    size_t oversample = numPIMs * OVERSAMPLING_RATE * OVERSAMPLING_RATE * OVERSAMPLING_RATE;
    size_t sampleCount = P * oversample;

    point** samples = malloc(sampleCount * sizeof(point*));
    if(!samples)
        return NULL;

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; ++i)
        samples[i] = points[rand() % n];


    uint16_t sketchLevels = (uint16_t)ceil(log2(P));

    KDNode* cacheForest = NULL;
    buildSketch(&cacheForest, samples, sampleCount, sketchLevels);

    free(samples);

    if(!cacheForest)
        return NULL;

    point*** perPimPoints = malloc(P * sizeof(point**));
    size_t* perPimCounts = calloc(P, sizeof(size_t));

    if(!perPimPoints || !perPimCounts)
    {
        free(perPimPoints);
        free(perPimCounts);

        return NULL;
    }

    traverseSketchAndAssign(cacheForest, points, n, P, NULL, perPimCounts);

    for(size_t i = 0; i < P; ++i)
    {
        perPimPoints[i] = malloc(perPimCounts[i] * sizeof(point*));

        if(!perPimPoints[i])
        {
            for(size_t j = 0; j < i; ++j)
                free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);

            return NULL;
        }
    }

    size_t* counters = calloc(P, sizeof(size_t));
    if(!counters)
    {
        for(size_t i = 0; i < P; i++)
            if(perPimPoints[i])
                free(perPimPoints[i]);

        free(perPimPoints);
        free(perPimCounts);

        return NULL;
    }

    for(size_t i = 0; i < n; ++i)
    {
        size_t leafIndex = getBucket(cacheForest, points[i]);
        size_t pimId = leafIndex % P;

        perPimPoints[pimId][counters[pimId]++] = points[i];
    }

    free(counters);

    DPUKernelArgs args = {
        .totalPoints = 0,
        .pointsPerDpu = 0,
        .dim = DIMENSIONS,
        .alpha = ALPHA,
        .beta = BETA,
    };

    for(size_t i = 0; i < P; ++i)
    {
        if(perPimCounts[i] == 0)
            continue;

        float* pointData = malloc(perPimCounts[i] * DIMENSIONS * sizeof(float));

        if(!pointData)
        {
            for(size_t j = 0; j < i; ++j)
                if(perPimPoints[j] && perPimCounts[j] > 0)
                    free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            free(counters);

            return NULL;
        }

        for(size_t j = 0; j < perPimCounts[i]; ++j)
            memcpy(&pointData[j * DIMENSIONS], perPimPoints[i][j]->coords, DIMENSIONS * sizeof(float));

        int ret = dpuTransferDataToDpu(dpuCtx, i, pointData, perPimCounts[i] * DIMENSIONS * sizeof(float), DPU_XFER_DEFAULT);
        free(pointData);
        if(ret != 0)
        {
            for(size_t j = 0; j < P; ++j)
                if(perPimPoints[j] && perPimCounts[j] > 0)
                    free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            free(counters);

            return NULL;
        }

        args.totalPoints = perPimCounts[i];

        ret = dpuLaunchSpecificDpu(dpuCtx, i, "tasklet", &args); // TO DO - tasklet
        if(ret != 0)
        {
            for(size_t j = 0; j < P; j++)
                if(perPimPoints[j])
                    free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            free(counters);

            return NULL;
        }
    }

    sendSketchToAllDpus(dpuCtx, cacheForest);

    size_t totalNodes;
    KDNode** subtrees = collectSubtreesFromDpus(dpuCtx, P, &totalNodes);

    if(!subtrees)
    {
        for(size_t i = 0; i < P; ++i)
            if(perPimPoints[i])
                free(perPimPoints[i]);

        free(perPimPoints);
        free(perPimCounts);
        free(counters);
        return NULL;
    }

    scatterReplicas(dpuCtx, subtrees, P, n, cacheForest, alloc);

    freeDpuAllocation(alloc);

    for(size_t i = 0; i < P; ++i)
        if(subtrees[i])
            freeKDTree(subtrees[i]);

    free(subtrees);

    for(size_t i = 0; i < P; ++i)
        free(perPimPoints[i]);

    free(perPimPoints);
    free(perPimCounts);

    KDTree* result = malloc(sizeof(KDTree));
    if(result)
    {
        result->root = cacheForest;
        result->totalPoints = n;
        result->totalNodes = 0;
    }

    return result;
}

static void traverseSketchAndAssign(KDNode* sketch, point** points, size_t n, size_t P, point*** perPimPoints, size_t* perPimCounts)
{
    if(!sketch || !points || n == 0 || P == 0 || !perPimCounts)
        return;

    if(perPimPoints == NULL)
    {
        #pragma omp parallel for
        for(size_t i = 0; i < n; ++i)
        {
            size_t leafIndex = getBucket(sketch, points[i]);
            size_t pimId = leafIndex % P;

            #pragma omp atomic
            ++perPimCounts[pimId];
        }
    }
    else
    {
        int maxThreads = omp_get_max_threads();
        size_t** localCounters = malloc(maxThreads * sizeof(size_t*));

        #pragma omp parallel
        {
            #pragma omp single
            {
                for(int i = 0; i < maxThreads; ++i)
                    localCounters[i] = calloc(P, sizeof(size_t));
            }
        }

        #pragma omp parallel
        {
            int threadId = omp_get_thread_num();

            #pragma omp for
            for(size_t i = 0; i < n; ++i)
            {
                size_t leafIndex = getBucket(sketch, points[i]);
                size_t pimId = leafIndex % P;

                size_t pos = localCounters[threadId][pimId];

                perPimPoints[pimId][pos] = points[i];
                ++localCounters[threadId][pimId];
            }
        }

        #pragma omp parallel
        {
            int threadId = omp_get_thread_num();
            size_t pimStart = (P * threadId) / maxThreads;
            size_t pimEnd = (P * (threadId + 1)) / maxThreads;

            for(size_t p = pimStart; p < pimEnd; ++p)
            {
                size_t total = 0;

                for(int t = 0; t < maxThreads; ++t)
                    total += localCounters[t][p];

                perPimCounts[p] = total;
            }
        }

        for(int t = 0; t < maxThreads; ++t)
            free(localCounters[t]);

        free(localCounters);
    }
}


static void* serializeTree(KDNode* root, size_t* size)
{
    if(!root || !size)
        return NULL;

    size_t totalSize = 0;
    serializeNodeSize(root, &totalSize);

    void* buffer = malloc(totalSize);
    if(!buffer)
        return NULL;

    uint8_t* ptr = (uint8_t*)buffer;
    serializeNodeData(root, &ptr);

    *size = totalSize;
    return buffer;
}

static void serializeNodeSize(KDNode* node, size_t* size)
{
    if(!node)
        return;

    *size += 1;

    if(node->type == INTERNAL)
    {
        *size += 1 + sizeof(float);

        serializeNodeSize(node->data.internal.left, size);
        serializeNodeSize(node->data.internal.right, size);
    }
    else
    {
        *size += sizeof(uint32_t);

        if(node->data.leaf.pointsCount > 0 && node->data.leaf.points)
            *size += node->data.leaf.pointsCount * DIMENSIONS * sizeof(float);
    }
}

static void serializeNodeData(KDNode* node, uint8_t** ptr)
{
    if(!node)
        return;

    if(node->type == INTERNAL)
    {
        **ptr = 0;
        ++(*ptr);

        **ptr = node->data.internal.splitDim;
        ++(*ptr);

        memcpy(*ptr, &node->data.internal.splitValue, sizeof(float));
        *ptr += sizeof(float);

        serializeNodeData(node->data.internal.left, ptr);
        serializeNodeData(node->data.internal.right, ptr);

    }
    else
    {
        **ptr = 1;
        ++(*ptr);

        uint32_t count = (uint32_t)node->data.leaf.pointsCount;
        memcpy(*ptr, &count, sizeof(uint32_t));
        *ptr += sizeof(uint32_t);

        if(count > 0 && node->data.leaf.points)
        {
            for(size_t i = 0; i < count; i++)
            {
                memcpy(*ptr, node->data.leaf.points[i].coords, DIMENSIONS * sizeof(float));
                *ptr += DIMENSIONS * sizeof(float);
            }
        }
    }
}

static void sendSketchToAllDpus(DPUContext* ctx, KDNode* sketch)
{
    size_t sketchSize;
    void* sketchData = serializeTree(sketch, &sketchSize);

    if(!sketchData)
        return;

    int ret = dpuBroadcastToAllDpus(ctx, "sketch", sketchData, sketchSize, DPU_XFER_DEFAULT);

    free(sketchData);
}


static KDNode** collectSubtreesFromDpus(DPUContext* dpuCtx, size_t P, size_t* totalNodes)
{
    KDNode** allSubtrees = malloc(P * sizeof(KDNode*));
    if(!allSubtrees)
        return NULL;

    *totalNodes = 0;

    for(size_t i = 0; i < P; ++i)
    {
        size_t treeSize = 0;
        int ret = dpuTransferDataFromDpu(dpuCtx, i, &treeSize, sizeof(size_t), DPU_XFER_DEFAULT);
        if(ret != 0)
        {
            for(size_t j = 0; j < i; ++j)
                if(allSubtrees[j])
                    free(allSubtrees[j]);

            free(allSubtrees);

            return NULL;
        }

        if(treeSize > 0)
        {
            void* treeData = malloc(treeSize);
            if(!treeData)
            {
                for(size_t j = 0; j < i; ++j)
                    if(allSubtrees[j]) free(allSubtrees[j]);

                free(allSubtrees);

                return NULL;
            }

            ret = dpuTransferDataFromDpu(dpuCtx, i, treeData, treeSize, DPU_XFER_DEFAULT);
            if(ret != 0)
            {
                free(treeData);

                for(size_t j = 0; j < i; ++j)
                    if(allSubtrees[j])
                        free(allSubtrees[j]);

                free(allSubtrees);

                return NULL;
            }

            allSubtrees[i] = deserializeTree(treeData, treeSize);
            free(treeData);

            if(allSubtrees[i])
                (*totalNodes) += calculateSubtreeSize(allSubtrees[i]);
        }
        else
            allSubtrees[i] = NULL;
    }

    return allSubtrees;
}

static void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity)
{
    if(!node)
        return;

    if(*count >= *capacity)
    {
        *capacity = (*capacity) ? (*capacity) * 2 : 1024;
        *refs = realloc(*refs, (*capacity) * sizeof(KDNode*));
        if(!*refs)
            return;
    }

    (*refs)[(*count)++] = node;

    if(node->type == INTERNAL)
    {
        collectNodeReferences(node->data.internal.left, refs, count, capacity);
        collectNodeReferences(node->data.internal.right, refs, count, capacity);
    }
}

static void assignNodesToGroups(KDNode* node, size_t totalPoints, uint8_t* groupLevels)
{
    if(!node)
        return;

    size_t subtreeSize = calculateSubtreeSize(node);

    uint8_t level = 0;
    size_t current = totalPoints;

    while(current > LEAF_WRAP_THRESHOLD && subtreeSize < current)
    {
        ++level;
        current = (size_t)log2(current);
    }

    ++groupLevels[level];

    if(node->type == INTERNAL)
    {
        assignNodesToGroups(node->data.internal.left, totalPoints, groupLevels);
        assignNodesToGroups(node->data.internal.right, totalPoints, groupLevels);
    }
}

static void scatterReplicas(DPUContext* dpuCtx, KDNode** subtrees, size_t P, size_t totalPoints, KDNode* cacheForest, DpuAllocation* alloc)
{
    if(!dpuCtx || !subtrees || !alloc)
        return;

    KDNode** allNodes = NULL;
    size_t nodeCount = 0;
    size_t nodeCapacity = 0;

    for(size_t i = 0; i < P; ++i)
        if(subtrees[i])
            collectNodeReferences(subtrees[i], &allNodes, &nodeCount, &nodeCapacity);

    if(!allNodes || nodeCount == 0)
    {
        free(allNodes);
        return;
    }


    uint8_t* nodeLevels = malloc(nodeCount * sizeof(uint8_t));
    if(!nodeLevels)
    {
        free(allNodes);
        return;
    }

    uint8_t maxLevel = 0;
    for(size_t i = 0; i < nodeCount; ++i)
    {
        size_t subtreeSize = calculateSubtreeSize(allNodes[i]);

        uint8_t level = 0;
        if(subtreeSize > LEAF_WRAP_THRESHOLD)
        {
            size_t current = totalPoints;
            while(current > LEAF_WRAP_THRESHOLD && subtreeSize < current)
            {
                ++level;
                current = (size_t)log2(current);
            }
        }

        nodeLevels[i] = level;
        if(level > maxLevel) maxLevel = level;
    }

    size_t* nodesPerLevel = calloc(maxLevel + 1, sizeof(size_t));
    if(!nodesPerLevel)
    {
        free(nodeLevels);
        free(allNodes);
        return;
    }

    for(size_t i = 0; i < nodeCount; ++i)
        nodesPerLevel[nodeLevels[i]]++;

    ReplicaInfo** perDpuReplicas = calloc(P, sizeof(ReplicaInfo*));
    size_t* perDpuCounts = calloc(P, sizeof(size_t));
    size_t* perDpuCapacities = calloc(P, sizeof(size_t));

    if(!perDpuReplicas || !perDpuCounts || !perDpuCapacities)
    {
        free(perDpuReplicas);
        free(perDpuCounts);
        free(perDpuCapacities);
        free(nodesPerLevel);
        free(nodeLevels);
        free(allNodes);
        return;
    }

    for(uint8_t level = 1; level <= maxLevel; ++level)
    {
        if(nodesPerLevel[level] == 0)
            continue;

        size_t replicasThisLevel = nodesPerLevel[level] * P;

        ReplicaInfo* levelReplicas = malloc(replicasThisLevel * sizeof(ReplicaInfo));
        if(!levelReplicas)
            continue;

        size_t levelReplicaCount = 0;

        for(size_t i = 0; i < nodeCount; ++i)
        {
            if(nodeLevels[i] != level)
                continue;

            for(size_t r = 0; r < P; ++r)
            {
                uint32_t targetDpu = (uint32_t)(rand() % P);

                size_t subtreeSerializedSize;
                void* subtreeData = serializeTree(allNodes[i], &subtreeSerializedSize);

                if(subtreeData)
                {
                    uint32_t offset = allocateOnDpu(alloc, targetDpu, subtreeSerializedSize);

                    int ret = dpuTransferDataToDpu(dpuCtx, targetDpu, subtreeData, subtreeSerializedSize, DPU_XFER_DEFAULT);

                    if(ret == 0)
                    {
                        if(perDpuCounts[targetDpu] >= perDpuCapacities[targetDpu])
                        {
                            size_t newCap = perDpuCapacities[targetDpu] ? perDpuCapacities[targetDpu] * 2 : 16;
                            ReplicaInfo* newArray = realloc(perDpuReplicas[targetDpu], newCap * sizeof(ReplicaInfo));
                            if(!newArray)
                            {
                                free(subtreeData);
                                continue;
                            }

                            perDpuReplicas[targetDpu] = newArray;
                            perDpuCapacities[targetDpu] = newCap;
                        }

                        size_t idx = perDpuCounts[targetDpu]++;

                        perDpuReplicas[targetDpu][idx].dpuId = targetDpu;
                        perDpuReplicas[targetDpu][idx].nodeOffset = offset;
                        perDpuReplicas[targetDpu][idx].nodeCount = (uint32_t)calculateSubtreeSize(allNodes[i]);
                        perDpuReplicas[targetDpu][idx].groupLevel = level;

                        levelReplicas[levelReplicaCount++] = perDpuReplicas[targetDpu][idx];
                    }

                    free(subtreeData);
                }
            }
        }

        free(levelReplicas);
    }

    for(size_t i = 0; i < P; ++i)
    {
        if(perDpuCounts[i] > 0)
        {
            dpuTransferDataToDpu(dpuCtx, (uint32_t)i, &perDpuCounts[i], sizeof(size_t), DPU_XFER_DEFAULT);
            dpuTransferDataToDpu(dpuCtx, (uint32_t)i, perDpuReplicas[i], perDpuCounts[i] * sizeof(ReplicaInfo), DPU_XFER_DEFAULT);

            free(perDpuReplicas[i]);
        }
    }

    if(cacheForest)
    {
        size_t sketchSize;
        void* sketchData = serializeTree(cacheForest, &sketchSize);
        if(sketchData)
        {
            allocateOnDpu(alloc, 0, sketchSize);
            dpuBroadcastToAllDpus(dpuCtx, "sketch", sketchData, sketchSize, DPU_XFER_DEFAULT);
            free(sketchData);
        }
    }

    free(perDpuReplicas);
    free(perDpuCounts);
    free(perDpuCapacities);
    free(nodesPerLevel);
    free(nodeLevels);
    free(allNodes);
}

static void* serializeNode(KDNode* node, size_t* size)
{
    if(!node || !size)
        return NULL;

    *size = 1;
    *size += sizeof(uint32_t);

    if(node->type == INTERNAL)
    {
        *size += 1;
        *size += sizeof(float);
    }
    else
    {
        *size += sizeof(uint32_t);
        if(node->data.leaf.pointsCount > 0)
            *size += node->data.leaf.pointsCount * DIMENSIONS * sizeof(float);
    }

    void* buffer = malloc(*size);
    if(!buffer)
        return NULL;

    uint8_t* ptr = (uint8_t*)buffer;

    *ptr = (node->type == INTERNAL) ? 0 : 1;
    ++ptr;

    uint32_t subtreeSize = (uint32_t)calculateSubtreeSize(node);
    memcpy(ptr, &subtreeSize, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    if(node->type == INTERNAL)
    {
        *ptr = node->data.internal.splitDim;
        ++ptr;

        memcpy(ptr, &node->data.internal.splitValue, sizeof(float));
        ptr += sizeof(float);
    }
    else
    {
        uint32_t count = (uint32_t)node->data.leaf.pointsCount;
        memcpy(ptr, &count, sizeof(uint32_t));
        ptr += sizeof(uint32_t);

        for(size_t i = 0; i < count; ++i)
        {
            memcpy(ptr, node->data.leaf.points[i].coords, DIMENSIONS * sizeof(float));
            ptr += DIMENSIONS * sizeof(float);
        }
    }

    return buffer;
}

static KDNode* deserializeTree(void* data, size_t size)
{
    if(!data || size == 0)
        return NULL;

    uint8_t* ptr = (uint8_t*)data;
    uint8_t* end = ptr + size;

    return deserializeNode(&ptr, end);
}

static KDNode* deserializeNode(uint8_t** ptr, uint8_t* end)
{
    if(*ptr >= end)
        return NULL;

    KDNode* node = malloc(sizeof(KDNode));
    if(!node) return NULL;

    node->type = (**ptr == 0) ? INTERNAL : LEAF;
    ++(*ptr);

    uint32_t subtreeSize;
    memcpy(&subtreeSize, *ptr, sizeof(uint32_t));
    (*ptr) += sizeof(uint32_t);

    if(node->type == INTERNAL)
    {
        node->data.internal.splitDim = **ptr;
        ++(*ptr);

        memcpy(&node->data.internal.splitValue, *ptr, sizeof(float));
        (*ptr) += sizeof(float);

        node->data.internal.left = deserializeNode(ptr, end);
        if(node->data.internal.left)
            node->data.internal.left->parent = node;

        node->data.internal.right = deserializeNode(ptr, end);
        if(node->data.internal.right)
            node->data.internal.right->parent = node;
    }
    else
    {
        memcpy(&node->data.leaf.pointsCount, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        if(node->data.leaf.pointsCount > 0)
        {
            node->data.leaf.points = malloc(node->data.leaf.pointsCount * sizeof(point));
            if(!node->data.leaf.points)
            {
                free(node);
                return NULL;
            }

            for(size_t i = 0; i < node->data.leaf.pointsCount; ++i)
            {
                memcpy(node->data.leaf.points[i].coords, *ptr, DIMENSIONS * sizeof(float));
                (*ptr) += DIMENSIONS * sizeof(float);
            }
        }
        else
            node->data.leaf.points = NULL;
    }

    return node;
}

static DpuAllocation* createDpuAllocation(size_t numDpus)
{
    DpuAllocation* alloc = malloc(sizeof(DpuAllocation));
    if(!alloc)
        return NULL;

    alloc->nextOffset = calloc(numDpus, sizeof(uint32_t));
    alloc->allocationCount = calloc(numDpus, sizeof(uint32_t));
    alloc->numDpus = numDpus;

    if(!alloc->nextOffset || !alloc->allocationCount)
    {
        free(alloc->nextOffset);
        free(alloc->allocationCount);
        free(alloc);
        return NULL;
    }

    return alloc;
}

static void freeDpuAllocation(DpuAllocation* alloc)
{
    if(alloc)
    {
        free(alloc->nextOffset);
        free(alloc->allocationCount);
        free(alloc);
    }
}

static uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size)
{
    if(dpuId >= alloc->numDpus)
        return UINT32_MAX; // CHECK

    uint32_t offset = alloc->nextOffset[dpuId];
    alloc->nextOffset[dpuId] += (uint32_t)size;
    alloc->allocationCount[dpuId]++;
    return offset;
}

#include <dpu.h>
#include <dpu_types.h>
#include <math.h>

#include "kdTree/utils.h"

#include "environment/init.h"

void sendSketchToAllDpus(KDNode* sketch)
{
    size_t sketchSize;
    void* sketchData = serializeTree(sketch, &sketchSize);

    if(!sketchData)
        return;

    dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;

    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));
    DPU_ASSERT(dpu_broadcast_to(set, "sketch", 0, sketchData, sketchSize,  DPU_XFER_DEFAULT | DPU_XFER_FROM_DPU));

    free(sketchData);
    dpu_free(set);
}

KDNode** collectSubtreesFromDpus(size_t* totalNodes)
{
    dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;

    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    KDNode** allSubtrees = malloc(nPim * sizeof(KDNode*));
    if(!allSubtrees)
    {
        dpu_free(dpu_set);
        return NULL;
    }

    *totalNodes = 0;

    for(size_t i = 0; i < nPim; ++i)
    {
        size_t treeSize = 0;

        struct dpu_set_t dpu;
        uint32_t currentId = 0;
        bool found = false;

        DPU_FOREACH(dpu_set, dpu)
        {
            if(currentId == i)
            {
                found = true;
                break;
            }

            ++currentId;
        }

        if(!found)
        {
            for(size_t j = 0; j < i; ++j)
                if(allSubtrees[j])
                    free(allSubtrees[j]);

            free(allSubtrees);
            dpu_free(dpu_set);

            return NULL;
        }

        DPU_ASSERT(dpu_prepare_xfer(dpu, &treeSize));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, sizeof(size_t), DPU_XFER_DEFAULT));

        if(treeSize > 0)
        {
            void* treeData = malloc(treeSize);
            if(!treeData)
            {
                for(size_t j = 0; j < i; ++j)
                    if(allSubtrees[j])
                        free(allSubtrees[j]);

                free(allSubtrees);
                dpu_free(dpu_set);

                return NULL;
            }

            DPU_ASSERT(dpu_prepare_xfer(dpu, treeData));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, treeSize, DPU_XFER_DEFAULT));

            allSubtrees[i] = deserializeTree(treeData, treeSize);
            free(treeData);

            if(allSubtrees[i])
                (*totalNodes) += calculateSubtreeSize(allSubtrees[i]);
        }
        else
            allSubtrees[i] = NULL;
    }

    dpu_free(dpu_set);
    return allSubtrees;
}

void collectNodeReferences(KDNode* node, KDNode*** refs, size_t* count, size_t* capacity)
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

void scatterReplica(KDNode** subtrees, KDNode* cacheForest, DpuAllocation* alloc)
{
    if(!subtrees || !alloc)
        return;

    dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    KDNode** allNodes = NULL;
    size_t totalPoints = getConfig()->nPoint;
    size_t nodeCount = 0;
    size_t nodeCapacity = 0;

    for(size_t i = 0; i < nPim; ++i)
        if(subtrees[i])
            collectNodeReferences(subtrees[i], &allNodes, &nodeCount, &nodeCapacity);

    if(!allNodes || nodeCount == 0)
    {
        free(allNodes);
        dpu_free(set);
        return;
    }

    uint8_t* nodeLevels = malloc(nodeCount * sizeof(uint8_t));
    if(!nodeLevels)
    {
        free(allNodes);
        dpu_free(set);
        return;
    }

    uint8_t maxLevel = 0;
    for(size_t i = 0; i < nodeCount; ++i)
    {
        size_t subtreeSize = calculateSubtreeSize(allNodes[i]);

        uint8_t level = 0;
        if(subtreeSize > getConfig()->leafWrapThreshold)
        {
            size_t current = totalPoints;
            while(current > getConfig()->leafWrapThreshold && subtreeSize < current)
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
        dpu_free(set);
        return;
    }

    for(size_t i = 0; i < nodeCount; ++i)
        nodesPerLevel[nodeLevels[i]]++;

    ReplicaInfo** perDpuReplicas = calloc(nPim, sizeof(ReplicaInfo*));
    size_t* perDpuCounts = calloc(nPim, sizeof(size_t));
    size_t* perDpuCapacities = calloc(nPim, sizeof(size_t));

    if(!perDpuReplicas || !perDpuCounts || !perDpuCapacities)
    {
        free(perDpuReplicas);
        free(perDpuCounts);
        free(perDpuCapacities);
        free(nodesPerLevel);
        free(nodeLevels);
        free(allNodes);
        dpu_free(set);
        return;
    }

    for(uint8_t level = 1; level <= maxLevel; ++level)
    {
        if(nodesPerLevel[level] == 0)
            continue;

        size_t replicasThisLevel = nodesPerLevel[level] * nPim;

        ReplicaInfo* levelReplicas = malloc(replicasThisLevel * sizeof(ReplicaInfo));
        if(!levelReplicas)
            continue;

        size_t levelReplicaCount = 0;

        for(size_t i = 0; i < nodeCount; ++i)
        {
            if(nodeLevels[i] != level)
                continue;

            for(size_t r = 0; r < nPim; ++r)
            {
                uint32_t targetDpu = (uint32_t)(rand() % nPim);

                size_t subtreeSerializedSize;
                void* subtreeData = serializeTree(allNodes[i], &subtreeSerializedSize);

                if(subtreeData)
                {
                    uint32_t offset = allocateOnDpu(alloc, targetDpu, subtreeSerializedSize);

                    struct dpu_set_t dpu;
                    uint32_t currentId = 0;
                    bool success = false;
                    DPU_FOREACH(set, dpu)
                    {
                        if(currentId == targetDpu)
                        {
                            DPU_ASSERT(dpu_prepare_xfer(dpu, subtreeData));
                            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "input", 0, subtreeSerializedSize, DPU_XFER_DEFAULT));
                            success = true;
                            break;
                        }

                        ++currentId;
                    }

                    if(success)
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

                        size_t index = perDpuCounts[targetDpu]++;

                        perDpuReplicas[targetDpu][index].dpuId = targetDpu;
                        perDpuReplicas[targetDpu][index].nodeOffset = offset;
                        perDpuReplicas[targetDpu][index].nodeCount = (uint32_t)calculateSubtreeSize(allNodes[i]);
                        perDpuReplicas[targetDpu][index].groupLevel = level;

                        levelReplicas[levelReplicaCount++] = perDpuReplicas[targetDpu][index];
                    }

                    free(subtreeData);
                }
            }
        }

        free(levelReplicas);
    }

    for(size_t i = 0; i < nPim; ++i)
    {
        if(perDpuCounts[i] > 0)
        {
            struct dpu_set_t dpu;
            uint32_t currentId = 0;

            DPU_FOREACH(set, dpu)
            {
                if(currentId == i)
                {
                    DPU_ASSERT(dpu_prepare_xfer(dpu, &perDpuCounts[i]));
                    DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "input", 0, sizeof(size_t), DPU_XFER_DEFAULT));

                    DPU_ASSERT(dpu_prepare_xfer(dpu, perDpuReplicas[i]));
                    DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "input", 0, perDpuCounts[i] * sizeof(ReplicaInfo), DPU_XFER_DEFAULT));
                    break;
                }

                ++currentId;
            }

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

            DPU_ASSERT(dpu_broadcast_to(set, "sketch", 0, sketchData, sketchSize, DPU_XFER_DEFAULT | DPU_XFER_FROM_DPU));
            free(sketchData);
        }
    }

    free(perDpuReplicas);
    free(perDpuCounts);
    free(perDpuCapacities);
    free(nodesPerLevel);
    free(nodeLevels);
    free(allNodes);

    dpu_free(set);
}

DpuAllocation* createDpuAllocation()
{
    DpuAllocation* alloc = malloc(sizeof(DpuAllocation));
    if(!alloc)
        return NULL;

    alloc->nextOffset = calloc(numDpus, sizeof(uint32_t));
    alloc->allocationCount = calloc(numDpus, sizeof(uint32_t));
    alloc->numDpus = getConfig()->nPim;

    if(!alloc->nextOffset || !alloc->allocationCount)
    {
        free(alloc->nextOffset);
        free(alloc->allocationCount);
        free(alloc);
        return NULL;
    }

    return alloc;
}

uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size)
{
    uint32_t offset = alloc->nextOffset[dpuId];
    alloc->nextOffset[dpuId] += (uint32_t)size;
    ++alloc->allocationCount[dpuId];
    return offset;
}

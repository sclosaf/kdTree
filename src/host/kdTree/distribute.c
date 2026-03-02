#include "kdTree/distribute.h"
#include "kdTree/deserialize.h"

void sendSketchToAllDpus(DPUContext* ctx, KDNode* sketch)
{
    size_t sketchSize;
    void* sketchData = serializeTree(sketch, &sketchSize);

    if(!sketchData)
        return;

    int ret = dpuBroadcastToAllDpus(ctx, "sketch", sketchData, sketchSize, DPU_XFER_DEFAULT);

    free(sketchData);
}

KDNode** collectSubtreesFromDpus(DPUContext* dpuCtx, size_t P, size_t* totalNodes)
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

void scatterReplica(DPUContext* dpuCtx, KDNode** subtrees, size_t P, size_t totalPoints, KDNode* cacheForest, DpuAllocation* alloc)
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

DpuAllocation* createDpuAllocation(size_t numDpus)
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

uint32_t allocateOnDpu(DpuAllocation* alloc, uint32_t dpuId, size_t size)
{
    uint32_t offset = alloc->nextOffset[dpuId];
    alloc->nextOffset[dpuId] += (uint32_t)size;
    alloc->allocationCount[dpuId]++;
    return offset;
}

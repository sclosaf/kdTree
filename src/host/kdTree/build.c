#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>
#include <omp.h>
#include <time.h>
#include <dpu.h>
#include <dpu_types.h>

#include "host/management/logging.h"

#include "host/environment/init.h"

#include "host/kdTree/build.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/counters.h"
#include "host/kdTree/free.h"
#include "host/kdTree/serialization.h"

#include "host/loader/dispatcher.h"

KDTree* buildOnChip(point** points, size_t size)
{
    logMessage("Building on-chip KD-tree", DEBUG);

    if(!points || size == 0)
    {
        logMessage("Invalid input: null points or zero size", ERROR);
        return NULL;
    }

    KDTree* tree = (KDTree*)malloc(sizeof(KDTree));
    if(!tree)
    {
        logMessage("Failed to allocate KDTree", ERROR);
        return NULL;
    }

    tree->totalPoints = size;
    tree->totalNodes = 0;

    char msg[256];

    if(size < getChunkSize() * getOversamplingRate())
    {
        logMessage("Input size below threshold, using plain tree build", DEBUG);
        tree->root = buildTreePlain(points, 0, size - 1, 0);
    }
    else
    {
        logMessage("Input size above threshold, using sketch-based tree build", DEBUG);
        tree->root = buildTree(points, size, 0);
    }

    if(!tree->root)
    {
        logMessage("Failed to build tree root", ERROR);
        free(tree);
        return NULL;
    }

    logMessage("Tree root built successfully", DEBUG);

    initializeSubtreeCounters(tree->root);
    logMessage("Subtree counters initialized", DEBUG);

    KDGroup** groups = logStarDecompose(tree->root);
    if(!groups)
    {
        logMessage("Failed to decompose tree into groups", ERROR);
        freeKDTree(tree->root);
        free(tree);
        return NULL;
    }

    logMessage("Log-star decomposition complete", DEBUG);

    KDTree* finalTree = replicate(tree, groups);

    for(size_t i = 0; groups[i] != NULL; ++i)
    {
        if(groups[i]->masterRoots)
            free(groups[i]->masterRoots);

        free(groups[i]);
    }

    free(groups);
    free(tree);

    if(!finalTree)
    {
        logMessage("Failed to build final replicated tree", ERROR);
        return NULL;
    }

    snprintf(msg, sizeof(msg), "On-chip KD-tree built successfully with %zu points", size);
    logMessage(msg, INFO);

    return finalTree;
}

KDGroup** logStarDecompose(KDNode* root)
{
    logMessage("Starting log-star decomposition from node", DEBUG);

    if(!root)
    {
        logMessage("Invalid root provided for decomposition", ERROR);
        return NULL;
    }

    char msg[256];
    uint32_t rootSize = getNodeSize(root);
    snprintf(msg, sizeof(msg), "Root type: %d, size: %u", root->type, rootSize);
    logMessage(msg, DEBUG);

    size_t P = getNPim();
    uint8_t numGroups = 0;
    size_t temp = P;

    while(temp > 1)
    {
        ++numGroups;
        temp = (size_t)log2(temp);
    }

    ++numGroups;

    snprintf(msg, sizeof(msg), "Number of groups computed: %u (log*P=%u)", numGroups, numGroups-1);
    logMessage(msg, INFO);

    KDGroup** groups = (KDGroup**)calloc(numGroups + 1, sizeof(KDGroup*));
    if(!groups)
    {
        logMessage("Failed to allocate groups array", ERROR);
        return NULL;
    }

    size_t* H = (size_t*)malloc((numGroups + 1) * sizeof(size_t));
    if(!H)
    {
        logMessage("Failed to allocate H array", ERROR);
        free(groups);
        return NULL;
    }

    H[0] = P;
    for(uint8_t i = 1; i <= numGroups; ++i)
    {
        H[i] = (size_t)log2(H[i-1]);
        if(H[i] < 1) H[i] = 1;
        snprintf(msg, sizeof(msg), "H[%d]=%zu", i, H[i]);
        logMessage(msg, DEBUG);
    }

    for(uint8_t j = 0; j < numGroups; ++j)
    {
        groups[j] = (KDGroup*)malloc(sizeof(KDGroup));
        if(!groups[j])
        {
            logMessage("Failed to allocate group", ERROR);
            for(uint8_t k = 0; k < j; ++k)
                free(groups[k]);

            free(groups);
            free(H);
            return NULL;
        }

        groups[j]->masterRoots = NULL;
        groups[j]->masterRootCount = 0;
        groups[j]->replicas = NULL;
        groups[j]->replicaCount = 0;

        if(j == 0)
        {
            groups[j]->minSize = H[0];
            groups[j]->maxSize = (size_t) - 1;
            snprintf(msg, sizeof(msg), "Group %u: size >= %f", j, groups[j]->minSize);
        }
        else
        {
            groups[j]->minSize = H[j];
            groups[j]->maxSize = H[j-1];
            snprintf(msg, sizeof(msg), "Group %u: %f <= size < %f", j, groups[j]->minSize, groups[j]->maxSize);
        }

        logMessage(msg, INFO);
    }

    groups[numGroups] = NULL;
    free(H);

    if(root && root->type == INTERNAL)
    {
        assignNodesToGroups(root, groups, numGroups);
        logMessage("Nodes assigned to groups", DEBUG);
    }

    return groups;
}

KDTree* replicate(KDTree* original, KDGroup** groups)
{
    logMessage("Replicating KD-tree", DEBUG);

    if(!original || !groups)
    {
        logMessage("Invalid original tree or groups provided", ERROR);
        return NULL;
    }

    KDTree* newTree = (KDTree*)malloc(sizeof(KDTree));
    if(!newTree)
    {
        logMessage("Failed to allocate replicated KDTree", ERROR);
        return NULL;
    }

    newTree->totalPoints = original->totalPoints;
    newTree->totalNodes = original->totalNodes;
    newTree->root = original->root;
    newTree->groups = groups;

    logMessage("Building group replicas", DEBUG);

    for(int i = 1; groups[i] != NULL; ++i)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Building replicas for group %d", i);
        logMessage(msg, DEBUG);

        buildGroupReplicas(groups[i]);
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "KD-tree replicated: %u points, %d nodes", newTree->totalPoints, newTree->totalNodes);
    logMessage(msg, INFO);

    return newTree;
}

KDTree* buildPIMkdtree(point** points, size_t size)
{
    logMessage("Building PIM KD-tree", DEBUG);

    if(!points || size == 0)
    {
        logMessage("Invalid input: null points or zero size", ERROR);
        return NULL;
    }

    uint32_t nPim = getNPim();
    char msg[256];

    DpuAllocation* alloc = createDpuAllocation();
    if(!alloc)
    {
        logMessage("Failed to create DPU allocation", ERROR);
        return NULL;
    }

    size_t rate = getOversamplingRate();
    size_t sampleCount = nPim * rate;
    if(sampleCount > size)
        sampleCount = size;

    snprintf(msg, sizeof(msg), "Sampling %zu points for sketch construction", sampleCount);
    logMessage(msg, INFO);

    point** samples = malloc(sampleCount * sizeof(point*));
    if(!samples)
    {
        logMessage("Failed to allocate samples array", ERROR);
        freeDpuAllocation(alloc);
        return NULL;
    }

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; ++i)
        samples[i] = points[rand() % size];

    uint16_t sketchLevels = (uint16_t)ceil(log2(nPim));
    if(sketchLevels > getSketchHeight())
        sketchLevels = getSketchHeight();

    snprintf(msg, sizeof(msg), "Building sketch with %u levels", sketchLevels);
    logMessage(msg, DEBUG);

    KDNode* cacheForest = NULL;
    buildSketch(&cacheForest, samples, sampleCount, sketchLevels);
    free(samples);

    if(!cacheForest)
    {
        logMessage("Failed to build sketch", ERROR);
        freeDpuAllocation(alloc);
        return NULL;
    }

    logMessage("Sketch built successfully", DEBUG);

    point*** perPimPoints = malloc(nPim * sizeof(point**));
    size_t* perPimCounts = calloc(nPim, sizeof(size_t));

    if(!perPimPoints || !perPimCounts)
    {
        logMessage("Failed to allocate per-PIM point arrays", ERROR);
        free(perPimPoints);
        free(perPimCounts);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        return NULL;
    }

    size_t* leafForDpu = (size_t*)malloc(nPim * sizeof(size_t));
    if(!leafForDpu)
    {
        logMessage("Failed to allocate leafForDpu array", ERROR);
        free(perPimPoints);
        free(perPimCounts);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        return NULL;
    }

    for(size_t i = 0; i < nPim; ++i)
        leafForDpu[i] = (size_t)-1;

    logMessage("Counting points per PIM and recording leaf mapping", DEBUG);

    #pragma omp parallel for
    for(size_t i = 0; i < size; ++i)
    {
        size_t leafIndex = getBucket(cacheForest, points[i]);
        size_t pimId = leafIndex % nPim;
        #pragma omp atomic
        ++perPimCounts[pimId];

        #pragma omp critical
        {
            if(leafForDpu[pimId] == (size_t)-1)
                leafForDpu[pimId] = leafIndex;
        }
    }

    for(size_t i = 0; i < nPim; ++i)
    {
        if(perPimCounts[i] > 0 && leafForDpu[i] == (size_t)-1)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "DPU %zu has points but no leaf mapping", i);
            logMessage(msg, ERROR);
        }
    }

    for(size_t i = 0; i < nPim; ++i)
    {
        if(perPimCounts[i] > 0)
        {
            perPimPoints[i] = malloc(perPimCounts[i] * sizeof(point*));
            if(!perPimPoints[i])
            {
                logMessage("Failed to allocate per-PIM point buffer", ERROR);
                for(size_t j = 0; j < i; ++j)
                    if(perPimPoints[j])
                        free(perPimPoints[j]);

                free(perPimPoints);
                free(perPimCounts);
                free(leafForDpu);
                freeKDTree(cacheForest);
                freeDpuAllocation(alloc);
                return NULL;
            }
        }
        else
            perPimPoints[i] = NULL;
    }

    logMessage("Per-PIM point buffers allocated", DEBUG);

    memset(perPimCounts, 0, nPim * sizeof(size_t));
    traverseSketchAndAssign(cacheForest, points, size, perPimPoints, perPimCounts);

    logMessage("Points assigned to PIMs", DEBUG);

    uint8_t** allOutputBuffers = NULL;
    size_t* allOutputSizes = NULL;
    uint64_t** allNodeMaps = NULL;
    size_t* allMapSizes = NULL;

    logMessage("Dispatching parallel build to DPUs", DEBUG);

    int ret = dispatchBuildTask(perPimPoints, perPimCounts, nPim, &allOutputBuffers, &allOutputSizes, &allNodeMaps, &allMapSizes);

    if(ret != 0)
    {
        logMessage("Parallel build task failed", ERROR);

        if(allOutputBuffers)
        {
            for(size_t i = 0; i < nPim; ++i)
                if(allOutputBuffers[i]) free(allOutputBuffers[i]);

            free(allOutputBuffers);
        }

        free(allOutputSizes);

        if(allNodeMaps)
        {
            for(size_t i = 0; i < nPim; ++i)
                if(allNodeMaps[i]) free(allNodeMaps[i]);

            free(allNodeMaps);
        }

        free(allMapSizes);

        for(size_t i = 0; i < nPim; ++i)
            if(perPimPoints[i]) free(perPimPoints[i]);

        free(perPimPoints);
        free(perPimCounts);
        free(leafForDpu);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        return NULL;
    }

    logMessage("All DPU build tasks completed", DEBUG);

    KDNode** subtrees = malloc(nPim * sizeof(KDNode*));
    if(!subtrees)
    {
        logMessage("Failed to allocate subtrees array", ERROR);
        for(size_t i = 0; i < nPim; ++i)
        {
            if(allOutputBuffers[i])
                free(allOutputBuffers[i]);

            if(allNodeMaps[i])
                free(allNodeMaps[i]);

            if(perPimPoints[i])
                free(perPimPoints[i]);
        }

        free(perPimPoints);
        free(perPimCounts);
        free(allOutputBuffers);
        free(allOutputSizes);
        free(allNodeMaps);
        free(allMapSizes);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        return NULL;
    }

    size_t totalNodes = 0;

    for(size_t i = 0; i < nPim; ++i)
    {
        if(allOutputSizes[i] > 0 && allOutputBuffers[i])
        {
            subtrees[i] = deserializeTree(allOutputBuffers[i], allOutputSizes[i]);
            free(allOutputBuffers[i]);

            if(subtrees[i])
            {
                snprintf(msg, sizeof(msg), "Subtree deserialized for PIM %zu", i);
                logMessage(msg, INFO);

                if(allMapSizes[i] > 0 && allNodeMaps[i])
                {
                    KDNode** nodeRefs = NULL;
                    size_t nodeCount = 0;
                    size_t nodeCapacity = 0;

                    collectNodeReferences(subtrees[i], &nodeRefs, &nodeCount, &nodeCapacity);

                    if(nodeRefs && nodeCount > 0)
                    {
                        size_t nodesToRegister = (nodeCount < allMapSizes[i]) ? nodeCount : allMapSizes[i];
                        for(size_t j = 0; j < nodesToRegister; ++j)
                            registerNodeLocation(nodeRefs[j], i, allNodeMaps[i][j]);

                        snprintf(msg, sizeof(msg), "Registered %zu node locations for PIM %zu", nodesToRegister, i);
                        logMessage(msg, INFO);

                        free(nodeRefs);
                    }

                    free(allNodeMaps[i]);
                }

                totalNodes += getNodeSize(subtrees[i]);
            }
            else
            {
                snprintf(msg, sizeof(msg), "Failed to deserialize subtree for PIM %zu", i);
                logMessage(msg, ERROR);
            }
        }
        else
            subtrees[i] = NULL;
    }

    snprintf(msg, sizeof(msg), "Total nodes across all subtrees: %zu", totalNodes);
    logMessage(msg, INFO);

    for(size_t i = 0; i < nPim; ++i)
        if(subtrees[i])
            initializeSubtreeCounters(subtrees[i]);

    logMessage("Subtree counters initialized", DEBUG);

    initNodeLocationMap();
    Data* data = getData();
    if(!data->map)
    {
        logMessage("Failed to initialize node location map", ERROR);
        for(size_t i = 0; i < nPim; ++i)
        {
            if(subtrees[i])
                freeKDTree(subtrees[i]);

            if(perPimPoints[i])
                free(perPimPoints[i]);
        }

        free(subtrees);
        free(perPimPoints);
        free(perPimCounts);
        free(allOutputBuffers);
        free(allOutputSizes);
        free(allNodeMaps);
        free(allMapSizes);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        return NULL;
    }

    logMessage("Node location map initialized", DEBUG);

    logMessage("Attaching subtrees to main sketch", DEBUG);

    uint16_t chunkSize = getChunkSize();

    for(size_t i = 0; i < nPim; ++i)
    {
        if(subtrees[i] && leafForDpu[i] != (size_t)-1)
        {
            uint32_t leafIndex = (uint32_t)leafForDpu[i];

            if(leafIndex >= chunkSize)
            {
                char msg[256];
                snprintf(msg, sizeof(msg), "ERROR: Invalid leaf index %u for DPU %zu (max %u)", leafIndex, i, chunkSize);
                logMessage(msg, ERROR);
                continue;
            }

            char msg[256];
            snprintf(msg, sizeof(msg), "Attaching subtree from PIM %zu to leaf %u (chunkSize=%u)", i, leafIndex, chunkSize);
            logMessage(msg, DEBUG);

            attachSubtree(cacheForest, leafIndex, subtrees[i]);
        }
        else if(subtrees[i] && leafForDpu[i] == (size_t)-1)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Skipping attachment for DPU %zu - no leaf mapping found", i);
            logMessage(msg, ERROR);
        }
    }

    free(leafForDpu);

    logMessage("All subtrees attached to main sketch", DEBUG);

    logMessage("Recomputing counters after attachment", DEBUG);
    initializeSubtreeCounters(cacheForest);

    scatterReplica(subtrees, cacheForest, alloc);
    logMessage("Replicas scattered to DPUs", DEBUG);

    freeDpuAllocation(alloc);

    for(size_t i = 0; i < nPim; ++i)
    {
        if(subtrees[i])
            freeKDTree(subtrees[i]);

        if(perPimPoints[i])
            free(perPimPoints[i]);
    }

    free(subtrees);
    free(perPimPoints);
    free(perPimCounts);
    free(allOutputBuffers);
    free(allOutputSizes);
    free(allNodeMaps);
    free(allMapSizes);

    KDTree* result = malloc(sizeof(KDTree));
    if(result)
    {
        result->root = cacheForest;
        result->totalPoints = size;
        result->totalNodes = totalNodes;

        snprintf(msg, sizeof(msg), "PIM KD-tree built successfully: %zu points, %zu nodes", size, totalNodes);
        logMessage(msg, INFO);
    }
    else
    {
        logMessage("Failed to allocate final PIM KD-tree result", ERROR);
        freeKDTree(cacheForest);
    }

    return result;
}

void assignNodesToGroups(KDNode* node, KDGroup** groups, uint8_t numGroups)
{
    logMessage("Assigning node to groups", DEBUG);

    if(!node || !groups)
    {
        logMessage("Null node or groups provided", ERROR);
        return;
    }

    size_t subtreeSize = getNodeSize(node);
    int16_t groupId = findGroup(subtreeSize, groups, numGroups);

    if(groupId >= 0 && groupId < numGroups && groups[groupId])
    {
        groups[groupId]->masterRoots = (KDNode**)realloc(groups[groupId]->masterRoots, (groups[groupId]->masterRootCount + 1) * sizeof(KDNode*));
        if(groups[groupId]->masterRoots)
        {
            groups[groupId]->masterRoots[groups[groupId]->masterRootCount++] = node;

            char msg[256];
            snprintf(msg, sizeof(msg), "Node of size %zu assigned to group %d (total in group: %zu)", subtreeSize, groupId, groups[groupId]->masterRootCount);
            logMessage(msg, INFO);
        }
        else
            logMessage("Failed to reallocate masterRoots for group", ERROR);
    }

    if(node->type == INTERNAL)
    {
        if(node->data.internal.left)
            assignNodesToGroups(node->data.internal.left, groups, numGroups);

        if(node->data.internal.right)
            assignNodesToGroups(node->data.internal.right, groups, numGroups);
    }
}

void copyNode(KDNode* dest, KDNode* src)
{
    logMessage("Copying KD node", DEBUG);

    if(!dest || !src)
    {
        logMessage("Null source or destination node", ERROR);
        return;
    }

    dest->type = src->type;
    dest->parent = NULL;

    if(src->type == INTERNAL)
    {
        dest->data.internal.splitDim = src->data.internal.splitDim;
        dest->data.internal.splitValue = src->data.internal.splitValue;
        dest->data.internal.approximateCounter = src->data.internal.approximateCounter;
        dest->data.internal.left = NULL;
        dest->data.internal.right = NULL;

        char msg[256];
        snprintf(msg, sizeof(msg), "Internal node copied: splitDim=%u, splitValue=%.4f", dest->data.internal.splitDim, dest->data.internal.splitValue);
        logMessage(msg, INFO);
    }
    else
    {
        dest->data.leaf.pointsCount = src->data.leaf.pointsCount;
        dest->data.leaf.points = (point*)malloc(dest->data.leaf.pointsCount * sizeof(point));

        if(dest->data.leaf.points)
        {
            memcpy(dest->data.leaf.points, src->data.leaf.points, dest->data.leaf.pointsCount * sizeof(point));

            char msg[256];
            snprintf(msg, sizeof(msg), "Leaf node copied: %zu points", dest->data.leaf.pointsCount);
            logMessage(msg, INFO);
        }
        else
            logMessage("Failed to allocate points buffer for leaf node copy", ERROR);
    }
}

KDNode* buildTree(point** points, size_t size, uint16_t depth)
{
    logMessage("Building sketch based KD-tree", DEBUG);

    if(size <= getLeafWrapThreshold())
    {
        logMessage("Size below leaf threshold, creating leaf node", DEBUG);
        return createLeafNode(points, size);
    }

    size_t sampleCount = getChunkSize() * getOversamplingRate();
    point** samples = (point**)malloc(sampleCount * sizeof(point*));
    if(!samples)
    {
        logMessage("Failed to allocate samples array", ERROR);
        return NULL;
    }

    logMessage("Samples array allocated successfully", DEBUG);

    #pragma omp parallel
    {
        unsigned int seed = time(NULL) ^ omp_get_thread_num();

        #pragma omp for
        for(size_t i = 0; i < sampleCount; ++i)
            samples[i] = points[rand_r(&seed) % size];
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Building sketch from %zu samples at depth %u", sampleCount, depth);
    logMessage(msg, DEBUG);

    KDNode* sketch = NULL;
    buildSketch(&sketch, samples, sampleCount, getSketchHeight());
    free(samples);

    if(!sketch)
    {
        logMessage("Failed to build sketch", ERROR);
        return NULL;
    }

    Bucket* buckets = sievePoints(points, size, sketch);
    if(!buckets)
    {
        logMessage("Failed to sieve points into buckets", ERROR);
        freeKDTree(sketch);
        return NULL;
    }

    logMessage("Points sieved into buckets", DEBUG);

    #pragma omp parallel for
    for(size_t i = 0; i < getChunkSize(); ++i)
    {
        if(buckets[i].size > 0)
        {
            KDNode* subtree = buildTree(buckets[i].bucket, buckets[i].size, depth + getSketchHeight());
            attachSubtree(sketch, i, subtree);
        }
    }

    free(buckets);

    snprintf(msg, sizeof(msg), "Sketch-based tree built at depth %u for %zu points", depth, size);
    logMessage(msg, INFO);

    return sketch;
}

Bucket* sievePoints(point** points, size_t size, KDNode* sketch)
{
    logMessage("Sieving points into buckets", DEBUG);

    uint16_t chunkSize = getChunkSize();
    size_t numChunks = (size + chunkSize - 1) / chunkSize;

    char msg[256];
    snprintf(msg, sizeof(msg), "Sieving %zu points into %zu chunks of size %u", size, numChunks, chunkSize);
    logMessage(msg, INFO);

    uint32_t** countMatrix = (uint32_t**)malloc(numChunks * sizeof(uint32_t*));
    if(!countMatrix)
    {
        logMessage("Failed to allocate count matrix", ERROR);
        return NULL;
    }

    #pragma omp parallel for
    for(size_t i = 0; i < numChunks; ++i)
    {
        countMatrix[i] = (uint32_t*)calloc(chunkSize, sizeof(uint32_t));

        size_t chunkStart = i * chunkSize;
        size_t chunkEnd = (chunkStart + chunkSize < size) ? (chunkStart + chunkSize) : size;

        for(size_t j = chunkStart; j < chunkEnd; ++j)
        {
            uint32_t bucketId = getBucket(sketch, points[j]);
            ++countMatrix[i][bucketId];
        }
    }

    logMessage("Count matrix filled", DEBUG);

    uint32_t** offsetMatrix = computePrefixSum(countMatrix, numChunks, chunkSize);
    if(!offsetMatrix)
    {
        logMessage("Failed to compute prefix sum matrix", ERROR);
        freeMatrix((void**)countMatrix, numChunks);
        return NULL;
    }

    for(size_t i = 0; i < numChunks; i++)
    {
        if(!offsetMatrix[i])
        {
            logMessage("Failed to compute a part of prefix sum matrix", ERROR);
            freeMatrix((void**)countMatrix, numChunks);
            freeMatrix((void**)offsetMatrix, numChunks);
            return NULL;
        }
    }

    uint32_t* bucketOffsets = (uint32_t*)malloc((getChunkSize() + 1) * sizeof(uint32_t));
    if(!bucketOffsets)
    {
        logMessage("Failed to allocate bucket offsets array", ERROR);
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    for(size_t j = 0; j < chunkSize; ++j)
        bucketOffsets[j] = offsetMatrix[0][j];

    bucketOffsets[chunkSize] = size;

    point** sortedPoints = (point**)malloc(size * sizeof(point*));
    if(!sortedPoints)
    {
        logMessage("Failed to allocate sorted points array", ERROR);
        free(bucketOffsets);
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    #pragma omp parallel for
    for(size_t i = 0; i < numChunks; ++i)
    {
        size_t chunkStart = i * chunkSize;
        size_t chunkEnd = (chunkStart + chunkSize < size) ? (chunkStart + chunkSize) : size;

        for(size_t j = chunkStart; j < chunkEnd; ++j)
        {
            uint32_t bucketId = getBucket(sketch, points[j]);
            uint32_t index = offsetMatrix[i][bucketId];
            sortedPoints[index] = points[j];
            ++offsetMatrix[i][bucketId];
        }
    }

    memcpy(points, sortedPoints, size * sizeof(point*));
    logMessage("Points sorted into buckets", DEBUG);

    Bucket* buckets = (Bucket*)malloc(chunkSize * sizeof(Bucket));
    if(!buckets)
    {
        logMessage("Failed to allocate buckets array", ERROR);
        free(sortedPoints);
        free(bucketOffsets);
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    for(size_t j = 0; j < getChunkSize(); ++j)
    {
        size_t start = bucketOffsets[j];
        size_t end = (j < chunkSize - 1) ? bucketOffsets[j + 1] : size;
        buckets[j].bucket = &points[start];
        buckets[j].size = end - start;
    }

    free(sortedPoints);
    free(bucketOffsets);
    freeMatrix((void**)countMatrix, numChunks);
    freeMatrix((void**)offsetMatrix, numChunks);

    logMessage("Buckets populated successfully", DEBUG);

    return buckets;
}

void buildSketch(KDNode** root, point** samples, size_t sampleCount, uint16_t levels)
{
    logMessage("Building sketch node", DEBUG);

    *root = (KDNode*)calloc(1, sizeof(KDNode));
    if(!*root)
    {
        logMessage("Failed to allocate sketch node", ERROR);
        return;
    }

    if(levels == 0)
    {
        (*root)->type = LEAF;
        (*root)->parent = NULL;
        (*root)->data.leaf.pointsCount = 0;
        (*root)->data.leaf.points = NULL;

        char msg[256];
        snprintf(msg, sizeof(msg), "Sketch leaf node created (empty bucket) at level 0");
        logMessage(msg, INFO);
        return;
    }

    (*root)->type = INTERNAL;
    (*root)->parent = NULL;
    (*root)->data.internal.approximateCounter = 0;
    (*root)->data.internal.left = NULL;
    (*root)->data.internal.right = NULL;

    if(sampleCount == 0)
    {
        logMessage("No samples to determine splitter", ERROR);
        (*root)->data.internal.splitDim = 0;
        (*root)->data.internal.splitValue = 0.0f;

        buildSketch(&(*root)->data.internal.left, NULL, 0, levels - 1);
        buildSketch(&(*root)->data.internal.right, NULL, 0, levels - 1);

        if((*root)->data.internal.left)
            (*root)->data.internal.left->parent = *root;
        if((*root)->data.internal.right)
            (*root)->data.internal.right->parent = *root;

        return;
    }

    uint8_t splitDim = findSplitDim(samples, 0, sampleCount - 1);
    float splitValue = findMedian(samples, 0, sampleCount - 1, splitDim);

    (*root)->data.internal.splitDim = splitDim;
    (*root)->data.internal.splitValue = splitValue;

    point** leftSamples = (point**)malloc(sampleCount * sizeof(point*));
    point** rightSamples = (point**)malloc(sampleCount * sizeof(point*));

    if(!leftSamples || !rightSamples)
    {
        logMessage("Failed to allocate sample partition buffers", ERROR);
        free(leftSamples);
        free(rightSamples);
        free(*root);
        *root = NULL;
        return;
    }

    size_t leftCount = 0;
    size_t rightCount = 0;

    for(size_t i = 0; i < sampleCount; ++i)
    {
        if(samples[i]->coords[splitDim] < splitValue)
            leftSamples[leftCount++] = samples[i];
        else
            rightSamples[rightCount++] = samples[i];
    }

    buildSketch(&(*root)->data.internal.left, leftSamples, leftCount, levels - 1);
    buildSketch(&(*root)->data.internal.right, rightSamples, rightCount, levels - 1);

    free(leftSamples);
    free(rightSamples);

    if((*root)->data.internal.left)
        (*root)->data.internal.left->parent = *root;
    if((*root)->data.internal.right)
        (*root)->data.internal.right->parent = *root;

    char msg[256];
    snprintf(msg, sizeof(msg), "Sketch internal node: splitDim=%u, splitValue=%.4f, levels=%u, leftSamples=%zu, rightSamples=%zu, counter=0",
             splitDim, splitValue, levels, leftCount, rightCount);
    logMessage(msg, INFO);
}

KDNode* buildTreePlain(point** points, size_t start, size_t end, uint16_t depth)
{
    if(end < start)
        return NULL;

    logMessage("Building plain KD-tree node", DEBUG);

    size_t size = end - start + 1;

    if(size <= getLeafWrapThreshold())
    {
        logMessage("Size below leaf threshold, creating leaf node", DEBUG);
        return createLeafNode(&points[start], size);
    }

    KDNode* node = (KDNode*)malloc(sizeof(KDNode));
    if(!node)
    {
        logMessage("Failed to allocate internal KD node", ERROR);
        return NULL;
    }

    uint8_t splitDim = findSplitDim(points, start, end);
    float splitValue = findMedian(points, start, end, splitDim);

    node->type = INTERNAL;
    node->parent = NULL;
    node->data.internal.splitDim = splitDim;
    node->data.internal.splitValue = splitValue;
    node->data.internal.approximateCounter = 0;
    node->data.internal.left = NULL;
    node->data.internal.right = NULL;

    char msg[256];
    snprintf(msg, sizeof(msg), "Plain node at depth %u: splitDim=%u, splitValue=%.4f, size=%zu", depth, splitDim, splitValue, size);
    logMessage(msg, INFO);

    size_t mid = partitionPoints(points, start, end, splitDim, splitValue);

    #pragma omp parallel sections
    {
        #pragma omp section
        {
            node->data.internal.left = buildTreePlain(points, start, mid - 1, depth + 1);
            if(node->data.internal.left)
                node->data.internal.left->parent = node;
        }
        #pragma omp section
        {
            node->data.internal.right = buildTreePlain(points, mid, end, depth + 1);
            if(node->data.internal.right)
                node->data.internal.right->parent = node;
        }
    }

    uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
    uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
    node->data.internal.approximateCounter = leftSize + rightSize;

    snprintf(msg, sizeof(msg), "Plain node built at depth %u: leftSize=%u, rightSize=%u", depth, leftSize, rightSize);
    logMessage(msg, INFO);

    return node;
}

KDNode* createLeafNode(point** points, size_t size)
{
    if(size == 0)
        return NULL;

    logMessage("Creating leaf node", DEBUG);

    KDNode* leaf = (KDNode*)malloc(sizeof(KDNode));
    if(!leaf)
    {
        logMessage("Failed to allocate leaf node", ERROR);
        return NULL;
    }

    leaf->type = LEAF;
    leaf->parent = NULL;

    leaf->data.leaf.points = (point*)malloc(size * sizeof(point));
    if(!leaf->data.leaf.points)
    {
        logMessage("Failed to allocate leaf points array", ERROR);
        free(leaf);
        return NULL;
    }

    for(size_t i = 0; i < size; ++i)
        leaf->data.leaf.points[i] = *points[i];

    leaf->data.leaf.pointsCount = size;

    char msg[256];
    snprintf(msg, sizeof(msg), "Leaf node created with %zu points", size);
    logMessage(msg, INFO);

    return leaf;
}

void attachSubtree(KDNode* sketch, uint16_t leafIndex, KDNode* subtree)
{
    logMessage("Attaching subtree to sketch", DEBUG);

    if(!sketch || !subtree)
    {
        logMessage("Null sketch or subtree provided", ERROR);
        return;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Attaching subtree to leaf index %u", leafIndex);
    logMessage(msg, DEBUG);

    KDNode* leafNode = findLeafNodeByIndex(sketch, leafIndex);

    if(!leafNode)
    {
        snprintf(msg, sizeof(msg), "Leaf node %u not found in sketch", leafIndex);
        logMessage(msg, ERROR);
        return;
    }

    if(leafNode->type != LEAF)
    {
        snprintf(msg, sizeof(msg), "Target node is not a leaf (type=%d)", leafNode->type);
        logMessage(msg, ERROR);
        return;
    }

    point* savedPoints = leafNode->data.leaf.points;
    size_t savedCount = leafNode->data.leaf.pointsCount;

    leafNode->type = INTERNAL;
    leafNode->data.internal.splitDim = 0;
    leafNode->data.internal.splitValue = 0.0f;
    leafNode->data.internal.approximateCounter = 0;
    leafNode->data.internal.left = subtree;
    leafNode->data.internal.right = NULL;

    subtree->parent = leafNode;

    uint32_t subtreeSize = getNodeSize(subtree);
    leafNode->data.internal.approximateCounter = subtreeSize;

    propagateCounterUpdate(leafNode, subtreeSize, false);

    if(savedPoints && savedCount > 0)
    {
        free(savedPoints);
        leafNode->data.leaf.points = NULL;
        leafNode->data.leaf.pointsCount = 0;
    }

    snprintf(msg, sizeof(msg), "Subtree attached to leaf %u, counter=%u", leafIndex, leafNode->data.internal.approximateCounter);
    logMessage(msg, INFO);
}

void traverseSketchAndAssign(KDNode* sketch, point** points, size_t n, point*** perPimPoints, size_t* perPimCounts)
{
    logMessage("Traversing sketch and assigning points to PIMs", DEBUG);

    if(!sketch || !points || n == 0 || !perPimCounts || !perPimPoints)
    {
        logMessage("Invalid input to traverseSketchAndAssign", ERROR);
        return;
    }

    uint8_t nPim = getNPim();
    int maxThreads = omp_get_max_threads();

    size_t** localCounters = malloc(maxThreads * sizeof(size_t*));

    #pragma omp parallel
    {
        #pragma omp single
        {
            for(int i = 0; i < maxThreads; ++i)
                localCounters[i] = calloc(nPim, sizeof(size_t));
        }
    }

    #pragma omp parallel
    {
        int threadId = omp_get_thread_num();

        #pragma omp for
        for(size_t i = 0; i < n; ++i)
        {
            size_t leafIndex = getBucket(sketch, points[i]);
            size_t pimId = leafIndex % nPim;

            size_t pos;

            #pragma omp atomic capture
            pos = perPimCounts[pimId]++;

            perPimPoints[pimId][pos] = points[i];

            localCounters[threadId][pimId]++;
        }
    }

    #pragma omp parallel
    {
        int threadId = omp_get_thread_num();
        size_t pimStart = (nPim * threadId) / maxThreads;
        size_t pimEnd = (nPim * (threadId + 1)) / maxThreads;

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

    char msg[256];
    snprintf(msg, sizeof(msg), "Sketch traversal complete: %zu points assigned across %u PIMs", n, nPim);
    logMessage(msg, INFO);
}

KDNodeReplica* createReplicaFromMaster(KDNode* masterNode)
{
    logMessage("Creating replica from master node", DEBUG);

    if(!masterNode)
    {
        logMessage("Null master node provided", ERROR);
        return NULL;
    }

    KDNodeReplica* replica = (KDNodeReplica*)calloc(1, sizeof(KDNodeReplica));
    if(!replica)
    {
        logMessage("Failed to allocate replica node", ERROR);
        return NULL;
    }

    replica->type = masterNode->type;
    replica->masterNode = masterNode;
    replica->parent = NULL;
    replica->descendants = NULL;
    replica->descendantCount = 0;
    replica->ancestors = NULL;
    replica->ancestorCount = 0;

    if(masterNode->type == INTERNAL)
    {
        replica->data.internal.left = NULL;
        replica->data.internal.right = NULL;
        logMessage("Internal replica created", DEBUG);
    }
    else
    {
        replica->data.leaf.pointsCount = masterNode->data.leaf.pointsCount;
        replica->data.leaf.points = (point*)malloc(replica->data.leaf.pointsCount * sizeof(point));

        if(replica->data.leaf.points)
        {
            memcpy(replica->data.leaf.points, masterNode->data.leaf.points, replica->data.leaf.pointsCount * sizeof(point));

            char msg[256];
            snprintf(msg, sizeof(msg), "Leaf replica created with %zu points", replica->data.leaf.pointsCount);
            logMessage(msg, INFO);
        }
        else
            logMessage("Failed to allocate points buffer for leaf replica", ERROR);
    }

    return replica;
}

void buildTopDownReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica)
{
    logMessage("Building top-down replica", DEBUG);

    if(!masterNode || !group || !replica)
    {
        logMessage("Null argument provided to buildTopDownReplica", ERROR);
        return;
    }

    if(masterNode->type == LEAF)
        return;

    size_t leftSize = 0, rightSize = 0;
    KDNode* leftChild = masterNode->data.internal.left;
    KDNode* rightChild = masterNode->data.internal.right;

    if(leftChild)
        leftSize = getNodeSize(leftChild);
    if(rightChild)
        rightSize = getNodeSize(rightChild);

    bool leftInGroup = (leftSize >= group->minSize && leftSize < group->maxSize);
    bool rightInGroup = (rightSize >= group->minSize && rightSize < group->maxSize);

    if(leftInGroup)
    {
        KDNodeReplica* leftReplica = createReplicaFromMaster(leftChild);
        if(leftReplica)
        {
            replica->data.internal.left = leftReplica;
            leftReplica->parent = replica;
            buildTopDownReplica(leftChild, group, leftReplica);

            replica->descendants = (KDNodeReplica**)realloc(replica->descendants, (replica->descendantCount + 1) * sizeof(KDNodeReplica*));
            if(replica->descendants)
            {
                replica->descendants[replica->descendantCount++] = leftReplica;

                char msg[256];
                snprintf(msg, sizeof(msg), "Left descendant added, total descendants: %zu", replica->descendantCount);
                logMessage(msg, DEBUG);
            }
            else
                logMessage("Failed to reallocate descendants array for left child", ERROR);
        }
        else
            logMessage("Failed to create left replica in top-down build", ERROR);
    }

    if(rightInGroup)
    {
        KDNodeReplica* rightReplica = createReplicaFromMaster(rightChild);
        if(rightReplica)
        {
            replica->data.internal.right = rightReplica;
            rightReplica->parent = replica;
            buildTopDownReplica(rightChild, group, rightReplica);

            replica->descendants = (KDNodeReplica**)realloc(replica->descendants, (replica->descendantCount + 1) * sizeof(KDNodeReplica*));
            if(replica->descendants)
            {
                replica->descendants[replica->descendantCount++] = rightReplica;

                char msg[256];
                snprintf(msg, sizeof(msg), "Right descendant added, total descendants: %zu", replica->descendantCount);
                logMessage(msg, DEBUG);
            }
            else
                logMessage("Failed to reallocate descendants array for right child", ERROR);
        }
        else
            logMessage("Failed to create right replica in top-down build", ERROR);
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Top-down replica built: leftInGroup=%d, rightInGroup=%d", leftInGroup, rightInGroup);
    logMessage(msg, INFO);
}

void buildBottomUpReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica)
{
    logMessage("Building bottom-up replica", DEBUG);

    if(!masterNode || !group || !replica)
    {
        logMessage("Null argument provided to buildBottomUpReplica", ERROR);
        return;
    }

    KDNode* currentAncestor = masterNode->parent;
    KDNodeReplica* currentReplica = replica;
    size_t ancestorsAdded = 0;

    while(currentAncestor)
    {
        size_t ancestorSize = getNodeSize(currentAncestor);

        if(ancestorSize >= group->minSize && ancestorSize < group->maxSize)
        {
            KDNodeReplica* ancestorReplica = createReplicaFromMaster(currentAncestor);
            if(!ancestorReplica)
            {
                logMessage("Failed to create ancestor replica in bottom-up build", ERROR);
                break;
            }

            if(currentAncestor->data.internal.left == currentReplica->masterNode)
                ancestorReplica->data.internal.left = currentReplica;
            else if(currentAncestor->data.internal.right == currentReplica->masterNode)
                ancestorReplica->data.internal.right = currentReplica;

            currentReplica->parent = ancestorReplica;

            replica->ancestors = (KDNodeReplica**)realloc(replica->ancestors, (replica->ancestorCount + 1) * sizeof(KDNodeReplica*));
            if(replica->ancestors)
            {
                replica->ancestors[replica->ancestorCount++] = ancestorReplica;
                ++ancestorsAdded;
            }
            else
            {
                logMessage("Failed to reallocate ancestors array", ERROR);
                break;
            }

            currentReplica = ancestorReplica;
            currentAncestor = currentAncestor->parent;
        }
        else
            break;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Bottom-up replica built: %zu ancestors added", ancestorsAdded);
    logMessage(msg, INFO);
}

void buildGroupReplicas(KDGroup* group)
{
    logMessage("Building replicas for group", DEBUG);

    if(!group || group->masterRootCount == 0)
    {
        logMessage("Null group or empty group provided", ERROR);
        return;
    }

    group->replicas = (KDNodeReplica**)calloc(group->masterRootCount, sizeof(KDNodeReplica*));
    if(!group->replicas)
    {
        logMessage("Failed to allocate replicas array", ERROR);
        return;
    }

    for(size_t i = 0; i < group->masterRootCount; ++i)
    {
        KDNode* masterNode = group->masterRoots[i];

        KDNodeReplica* replica = createReplicaFromMaster(masterNode);
        if(!replica)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Failed to create replica for master root %zu", i);
            logMessage(msg, ERROR);
            continue;
        }

        buildTopDownReplica(masterNode, group, replica);
        buildBottomUpReplica(masterNode, group, replica);

        group->replicas[i] = replica;
        ++group->replicaCount;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Group replicas built: %zu/%zu successful", group->replicaCount, group->masterRootCount);
    logMessage(msg, INFO);
}


#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>
#include <omp.h>
#include <dpu.h>
#include <dpu_types.h>

#include "environment/init.h"

#include "kdTree/build.h"
#include "kdTree/utils.h"

KDTree* onChipBuild(point** points, size_t size)
{
    if(!points || size == 0)
        return NULL;

    KDTree* tree = (KDTree*)malloc(sizeof(KDTree));
    if(!tree)
        return NULL;

    tree->totalPoints = size;
    tree->totalNodes = 0;

    if(size < getConfig()->chunkSize * getConfig()->oversamplingRate)
        tree->root = buildTreeParallelPlain(points, 0, size - 1, 0);
    else
        tree->root = buildTreeParallel(points, size, 0);

    if(!tree->root)
    {
        free(tree);
        return NULL;
    }

    initializeSubtreeCounters(tree->root, size);

    KDGroup** groups = logStarDecompose(tree);
    if(!groups)
    {
        freeKDTree(tree->root);
        free(tree);
        return NULL;
    }

    KDTree* finalTree = replicate(tree, groups);

    for(size_t i = 0; groups[i] != NULL; ++i)
    {
        if(groups[i]->rootNodes)
            free(groups[i]->rootNodes);
        free(groups[i]);
    }

    free(groups);

    freeKDTree(tree->root);
    free(tree);

    return finalTree;
}

KDGroup** logStarDecompose(KDTree* tree)
{
    if(!tree || !tree->root)
        return NULL;

    size_t totalSize = tree->totalPoints;
    uint8_t numGroups = 0;

    size_t current = totalSize;
    while(current > getConfig()->leafWrapThreshold)
    {
        ++numGroups;
        current = (size_t)log2(current);
    }
    ++numGroups;

    KDGroup** groups = (KDGroup**)malloc((numGroups) * sizeof(KDGroup*));
    if(!groups)
        return NULL;

    for(size_t i = 0; i < numGroups; ++i)
    {
        groups[i] = (KDGroup*)malloc(sizeof(KDGroup));
        if(!groups[i])
        {
            for(size_t j = 0; j < i; ++j)
                free(groups[j]);
            free(groups);
            return NULL;
        }

        groups[i]->rootNodes = NULL;
        groups[i]->count = 0;
        groups[i]->minSize = (i == 0) ? 0 : pow(2, pow(2, i - 1));
        groups[i]->maxSize = pow(2, pow(2, i));
    }

    assignNodesToGroups(tree->root, groups, numGroups);

    return groups;
}

KDTree* replicate(KDTree* original, KDGroup** groups)
{
    if(!original || !groups)
        return NULL;

    KDTree* newTree = (KDTree*)malloc(sizeof(KDTree));
    if(!newTree)
        return NULL;

    newTree->totalPoints = original->totalPoints;
    newTree->totalNodes = 0;

    newTree->root = buildReplicatedTree(groups, 0);

    return newTree;
}

KDTree* buildPIMKDTree(point** points, size_t n)
{
    if(!points || n == 0)
        return NULL;

    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    DpuAllocation* alloc = createDpuAllocation(nPim);
    if(!alloc)
    {
        dpu_free(set);
        return NULL;
    }

    size_t rate = getConfig()->oversamplingRate;
    size_t sampleCount = nPim * rate;

    point** samples = malloc(sampleCount * sizeof(point*));
    if(!samples)
    {
        freeDpuAllocation(alloc);
        dpu_free(set);
        return NULL;
    }

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; ++i)
        samples[i] = points[rand() % n];

    uint16_t sketchLevels = (uint16_t)ceil(log2(nPim));

    KDNode* cacheForest = NULL;
    buildSketch(&cacheForest, samples, sampleCount, sketchLevels);

    free(samples);

    if(!cacheForest)
    {
        freeDpuAllocation(alloc);
        dpu_free(set);
        return NULL;
    }

    point*** perPimPoints = malloc(nPim * sizeof(point**));
    size_t* perPimCounts = calloc(nPim, sizeof(size_t));

    if(!perPimPoints || !perPimCounts)
    {
        free(perPimPoints);
        free(perPimCounts);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        dpu_free(set);
        return NULL;
    }

    #pragma omp parallel for
    for(size_t i = 0; i < n; ++i)
    {
        size_t leafIndex = getBucket(cacheForest, points[i]);
        size_t pimId = leafIndex % nPim;

        #pragma omp atomic
        ++perPimCounts[pimId];
    }

    for(size_t i = 0; i < nPim; ++i)
    {
        perPimPoints[i] = malloc(perPimCounts[i] * sizeof(point*));

        if(!perPimPoints[i])
        {
            for(size_t j = 0; j < i; ++j)
                free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            freeKDTree(cacheForest);
            freeDpuAllocation(alloc);
            dpu_free(set);
            return NULL;
        }
    }

    memset(perPimCounts, 0, nPim * sizeof(size_t));
    traverseSketchAndAssign(cacheForest, points, n, perPimPoints, perPimCounts);

    for(size_t i = 0; i < nPim; ++i)
    {
        if(perPimCounts[i] == 0)
            continue;

        float* pointData = malloc(perPimCounts[i] * getConfig()->dimensions * sizeof(float));

        if(!pointData)
        {
            for(size_t j = 0; j < nPim; ++j)
                if(perPimPoints[j] && perPimCounts[j] > 0)
                    free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            freeKDTree(cacheForest);
            freeDpuAllocation(alloc);
            dpu_free(set);
            return NULL;
        }

        for(size_t j = 0; j < perPimCounts[i]; ++j)
            memcpy(&pointData[j * getConfig()->dimensions], perPimPoints[i][j]->coords, getConfig()->dimensions * sizeof(float));

        struct dpu_set_t dpu;
        uint32_t currentId = 0;
        bool found = false;

        DPU_FOREACH(set, dpu)
        {
            if(currentId == i)
            {
                found = true;
                DPU_ASSERT(dpu_prepare_xfer(dpu, pointData));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "input", 0, perPimCounts[i] * getConfig()->dimensions * sizeof(float), DPU_XFER_DEFAULT));
                break;
            }
            ++currentId;
        }

        free(pointData);

        if(!found)
        {
            for(size_t j = 0; j < nPim; ++j)
                if(perPimPoints[j] && perPimCounts[j] > 0)
                    free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            freeKDTree(cacheForest);
            freeDpuAllocation(alloc);
            dpu_free(set);
            return NULL;
        }

        currentId = 0;
        found = false;
        DPU_FOREACH(dpu_set, dpu)
        {
            if(currentId == i)
            {
                found = true;

                uint32_t totalPoints = perPimCounts[i];

                DPU_ASSERT(dpu_load(dpu, "tasklet", NULL));
                DPU_ASSERT(dpu_prepare_xfer(dpu, &totalPoints));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "totalPoints", 0,  sizeof(uint32_t), DPU_XFER_DEFAULT));
                DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
                break;
            }

            ++currentId;
        }

        if(!found)
        {
            for(size_t j = 0; j < nPim; ++j)
                if(perPimPoints[j])
                    free(perPimPoints[j]);

            free(perPimPoints);
            free(perPimCounts);
            freeKDTree(cacheForest);
            freeDpuAllocation(alloc);
            dpu_free(set);
            return NULL;
        }
    }

    size_t sketchSize;
    void* sketchData = serializeTree(cacheForest, &sketchSize);
    if(sketchData)
    {
        DPU_ASSERT(dpu_broadcast_to(set, "sketch", 0, sketchData, sketchSize, DPU_XFER_DEFAULT | DPU_XFER_FROM_DPU));
        free(sketchData);
    }

    size_t totalNodes;
    KDNode** subtrees = collectSubtreesFromDpus(&totalNodes);

    if(!subtrees)
    {
        for(size_t i = 0; i < nPim; ++i)
            if(perPimPoints[i])
                free(perPimPoints[i]);

        free(perPimPoints);
        free(perPimCounts);
        freeKDTree(cacheForest);
        freeDpuAllocation(alloc);
        dpu_free(set);
        return NULL;
    }

    for(size_t i = 0; i < nPim; ++i)
        if(subtrees[i])
            initializeSubtreeCounters(subtrees[i], n);

    scatterReplica(subtrees, n, cacheForest, alloc);

    freeDpuAllocation(alloc);

    for(size_t i = 0; i < nPim; ++i)
        if(subtrees[i])
            freeKDTree(subtrees[i]);

    free(subtrees);

    for(size_t i = 0; i < nPim; ++i)
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

    dpu_free(set);
    return result;
}

void assignNodesToGroups(KDNode* node, KDGroup** groups, uint8_t numGroups)
{
    if(!node)
        return;

    if(node->type == INTERNAL)
    {
        size_t subtreeSize = calculateSubtreeSize(node);

        int16_t groupId = findGroup(subtreeSize, groups, numGroups);

        if(groupId >= 0)
        {
            groups[groupId]->rootNodes = realloc(groups[groupId]->rootNodes, (groups[groupId]->count + 1) * sizeof(KDNode*));
            groups[groupId]->rootNodes[groups[groupId]->count++] = node;
        }

        assignNodesToGroups(node->data.internal.left, groups, numGroups);
        assignNodesToGroups(node->data.internal.right, groups, numGroups);
    }
}

KDNode* buildReplicatedTree(KDGroup** groups, uint8_t groupLevel)
{
    uint8_t currentLevel = groupLevel;
    while(groups[currentLevel] != NULL && groups[currentLevel]->count == 0)
        ++currentLevel;

    if(groups[currentLevel] == NULL)
        return NULL;

    KDGroup* currentGroup = groups[currentLevel];

    KDNode* node = (KDNode*)malloc(sizeof(KDNode));
    if(!node)
        return NULL;

    if(currentLevel == 0)
    {
        KDNode* originalNode = currentGroup->rootNodes[0];
        copyNode(node, originalNode);
    }
    else
    {
        node->type = INTERNAL;
        node->parent = NULL;

        KDNode* templateNode = currentGroup->rootNodes[0];
        node->data.internal.splitDim = templateNode->data.internal.splitDim;
        node->data.internal.splitValue = templateNode->data.internal.splitValue;
        node->data.internal.approximateCounter = 0;
        node->data.internal.left = NULL;
        node->data.internal.right = NULL;

        node->data.internal.left = buildReplicatedTree(groups, currentLevel + 1);
        node->data.internal.right = buildReplicatedTree(groups, currentLevel + 1);

        if(node->data.internal.left)
            node->data.internal.left->parent = node;

        if(node->data.internal.right)
            node->data.internal.right->parent = node;

        uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
        uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
        node->data.internal.approximateCounter = leftSize + rightSize;
    }

    return node;
}

void copyNode(KDNode* dest, KDNode* src)
{
    if(!dest || !src)
        return;

    dest->type = src->type;
    dest->parent = NULL;

    if(src->type == INTERNAL)
    {
        dest->data.internal.splitDim = src->data.internal.splitDim;
        dest->data.internal.splitValue = src->data.internal.splitValue;
        dest->data.internal.approximateCounter = src->data.internal.approximateCounter;
        dest->data.internal.left = NULL;
        dest->data.internal.right = NULL;
    }
    else
    {
        dest->data.leaf.pointsCount = src->data.leaf.pointsCount;
        dest->data.leaf.points = (point*)malloc(dest->data.leaf.pointsCount * sizeof(point));

        if(dest->data.leaf.points)
            memcpy(dest->data.leaf.points, src->data.leaf.points, dest->data.leaf.pointsCount * sizeof(point));
    }
}


KDNode* buildTreeParallel(point** points, size_t size, uint16_t depth)
{
    if(size <= getConfig()->leafWrapThreshold)
        return createLeafNode(points, size);

    size_t sampleCount = getConfig()->chunkSize * getConfig()->oversamplingRate;
    point** samples = (point**)malloc(sampleCount * sizeof(point*));
    if(!samples)
        return NULL;

    #pragma omp parallel for
    for(size_t i = 0; i < sampleCount; ++i)
        samples[i] = points[rand() % size];

    KDNode* sketch = NULL;
    buildSketch(&sketch, samples, sampleCount, getConfig()->sketchHeight);
    free(samples);

    Bucket* buckets = sievePoints(points, size, sketch);
    if(!buckets)
    {
        freeKDTree(sketch);
        return NULL;
    }

    #pragma omp parallel for
    for(size_t i = 0; i < getConfig()->chunkSize; ++i)
    {
        if(buckets[i].size > 0)
        {
            KDNode* subtree = buildTreeParallel(buckets[i].bucket, buckets[i].size, depth + getConfig()->sketchHeight);
            attachSubtree(sketch, i, subtree);
        }
    }

    if(sketch)
        initializeSubtreeCounters(sketch, size);

    free(buckets);
    return sketch;
}

Bucket* sievePoints(point** points, size_t size, KDNode* sketch)
{
    uint16_t chunkSize = getConfig()->chunkSize;
    size_t numChunks = (size + chunkSize - 1) / chunkSize;

    uint32_t** countMatrix = (uint32_t**)malloc(numChunks * sizeof(uint32_t*));
    if(!countMatrix)
        return NULL;

    #pragma omp parallel for
    for(size_t i = 0; i < numChunks; ++i)
    {
        countMatrix[i] = (uint32_t*)calloc(chunkSize, sizeof(uint32_t));

        size_t chunkStart = i * chunkSize;
        size_t chunkEnd = (chunkStart + chunkSize< size) ? (chunkStart + chunkSize) : size;

        for(size_t j = chunkStart; j < chunkEnd; ++j)
        {
            uint32_t bucketId = getBucket(sketch, points[j]);
            ++countMatrix[i][bucketId];
        }
    }

    uint32_t** offsetMatrix = computePrefixSum(countMatrix, numChunks, chunkSize);
    if(!offsetMatrix)
    {
        freeMatrix((void**)countMatrix, numChunks);
        return NULL;
    }

    uint32_t* bucketOffsets = (uint32_t*)malloc(getConfig()->chunkSize * sizeof(uint32_t));

    if(!bucketOffsets)
    {
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    for(size_t j = 0; j < chunkSize; ++j)
        bucketOffsets[j] = offsetMatrix[0][j];

    point** sortedPoints = (point**)malloc(size * sizeof(point*));
    if(!sortedPoints)
    {
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

    Bucket* buckets = (Bucket*)malloc(chunkSize * sizeof(Bucket));

    if(!buckets)
    {
        free(sortedPoints);
        free(bucketOffsets);
        freeMatrix((void**)countMatrix, numChunks);
        freeMatrix((void**)offsetMatrix, numChunks);
        return NULL;
    }

    for(size_t j = 0; j < getConfig()->chunkSize; ++j)
    {
        size_t start = bucketOffsets[j];
        size_t end = (j < chunkSize- 1) ? bucketOffsets[j + 1] : size;

        buckets[j].bucket = &points[start];
        buckets[j].size = end - start;
    }

    free(sortedPoints);
    free(bucketOffsets);

    freeMatrix((void**)countMatrix, numChunks);
    freeMatrix((void**)offsetMatrix, numChunks);

    return buckets;
}

void buildSketch(KDNode** root, point** samples, size_t sampleCount, uint16_t levels)
{
    if(levels == 0 || sampleCount <= getConfig()->leafWrapThreshold)
    {
        *root = (KDNode*)malloc(sizeof(KDNode));
        if(!*root)
            return;

        (*root)->type = LEAF;
        (*root)->parent = NULL;
        (*root)->data.leaf.points = NULL;
        (*root)->data.leaf.pointsCount = 0;

        return;
    }

    *root = (KDNode*)malloc(sizeof(KDNode));
    if(!*root)
        return;

    uint8_t splitDim = findSplitDim(samples, 0, sampleCount - 1);
    float splitValue = findMedian(samples, 0, sampleCount - 1, splitDim);

    (*root)->type = INTERNAL;
    (*root)->data.internal.splitDim = splitDim;
    (*root)->data.internal.splitValue = splitValue;
    (*root)->data.internal.approximateCounter = 0;
    (*root)->data.internal.left = NULL;
    (*root)->data.internal.right = NULL;
    (*root)->parent = NULL;

    size_t leftCount = 0;
    size_t rightCount = 0;

    point** leftSamples = (point**)malloc(sampleCount * sizeof(point*));
    point** rightSamples = (point**)malloc(sampleCount * sizeof(point*));

    if(!leftSamples || !rightSamples)
    {
        free(leftSamples);
        free(rightSamples);
        free(*root);
        *root = NULL;
        return;
    }

    for(size_t i = 0; i < sampleCount; ++i)
    {
        if(samples[i]->coords[splitDim] < splitValue)
            leftSamples[leftCount++] = samples[i];
        else
            rightSamples[rightCount++] = samples[i];
    }

    buildSketch(&(*root)->data.internal.left, leftSamples, leftCount, levels - 1);
    buildSketch(&(*root)->data.internal.right, rightSamples, rightCount, levels - 1);

    if((leftCount > 0 && !(*root)->data.internal.left) || (rightCount > 0 && !(*root)->data.internal.right))
    {
        if((*root)->data.internal.left)
            freeKDTree((*root)->data.internal.left);

        if((*root)->data.internal.right)
            freeKDTree((*root)->data.internal.right);

        free(leftSamples);
        free(rightSamples);
        free(*root);
        *root = NULL;
        return;
    }

    if((*root)->data.internal.left)
        (*root)->data.internal.left->parent = *root;

    if((*root)->data.internal.right)
        (*root)->data.internal.right->parent = *root;

    uint32_t leftSize = (*root)->data.internal.left ? getNodeSize((*root)->data.internal.left) : 0;
    uint32_t rightSize = (*root)->data.internal.right ? getNodeSize((*root)->data.internal.right) : 0;
    (*root)->data.internal.approximateCounter = leftSize + rightSize;

    free(leftSamples);
    free(rightSamples);
}

KDNode* buildTreeParallelPlain(point** points, size_t start, size_t end, uint16_t depth)
{
    size_t size = end - start + 1;

    if(size <= getConfig()->leafWrapThreshold)
        return createLeafNode(&points[start], size);

    KDNode* node = (KDNode*)malloc(sizeof(KDNode));
    if(!node)
        return NULL;

    uint8_t splitDim = findSplitDim(points, start, end);
    float splitValue = findMedian(points, start, end, splitDim);

    node->type = INTERNAL;
    node->parent = NULL;
    node->data.internal.splitDim = splitDim;
    node->data.internal.splitValue = splitValue;
    node->data.internal.approximateCounter = 0;
    node->data.internal.left = NULL;
    node->data.internal.right = NULL;

    size_t mid = parallelPartition(points, start, end, splitDim, splitValue);

    #pragma omp parallel sections
    {
        #pragma omp section
        {
            node->data.internal.left = buildTreeParallelPlain(points, start, mid - 1, depth + 1);
            if(node->data.internal.left)
                node->data.internal.left->parent = node;
        }
        #pragma omp section
        {
            node->data.internal.right = buildTreeParallelPlain(points, mid, end, depth + 1);
            if(node->data.internal.right)
                node->data.internal.right->parent = node;
        }
    }

    uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
    uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

    node->data.internal.approximateCounter = leftSize + rightSize;

    return node;
}

KDNode* createLeafNode(point** points, size_t size)
{
    KDNode* leaf = (KDNode*)malloc(sizeof(KDNode));
    if(!leaf)
        return NULL;

    leaf->type = LEAF;
    leaf->parent = NULL;

    leaf->data.leaf.points = (point*)malloc(size * sizeof(point));
    if(!leaf->data.leaf.points)
    {
        free(leaf);
        return NULL;
    }

    for(size_t i = 0; i < size; ++i)
        leaf->data.leaf.points[i] = *points[i];

    leaf->data.leaf.pointsCount = size;

    return leaf;
}

void attachSubtree(KDNode* sketch, uint16_t bucketId, KDNode* subtree)
{
    if(!sketch || bucketId >= getConfig()->chunkSize)
        return;

    KDNode* current = sketch;
    for(uint16_t level = getConfig()->sketchHeight - 1; level > 0; level--)
    {
        bool bit = (bucketId >> level) & 1;

        if(bit == 0)
            current = current->data.internal.left;
        else
            current = current->data.internal.right;
    }

    bool bit = bucketId & 1;

    if(bit == 0)
    {
        if(current->data.internal.left)
            freeLeafNode(current->data.internal.left);

        current->data.internal.left = subtree;
    }
    else
    {
        if(current->data.internal.right)
            freeLeafNode(current->data.internal.right);

        current->data.internal.right = subtree;
    }

    if(subtree)
        subtree->parent = current;
}

void traverseSketchAndAssign(KDNode* sketch, point** points, size_t n, point*** perPimPoints, size_t* perPimCounts)
{
    if(!sketch || !points || n == 0 || !perPimCounts || !perPimPoints)
        return;

    uint8_t nPim = getConfig()->nPim;

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

            size_t pos = localCounters[threadId][pimId];

            perPimPoints[pimId][pos] = points[i];
            ++localCounters[threadId][pimId];
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
}


#include <alloc.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <float.h>

#include "dpu/kdTree/build.h"
#include "dpu/kdTree/counters.h"
#include "dpu/kdTree/utils.h"

#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"

KDNode* buildOnChip(point** points, size_t size)
{
    if(!points || size == 0)
    {
        DPU_LOG("buildOnChip: ERR - Invalid parameters (points=%p, size=%zu)", points, size);
        return NULL;
    }

    KDNode* root = NULL;

    if(size < getChunkSize() * getOversamplingRate())
    {
        DPU_LOG("buildOnChip: Using plain tree build");
        root = buildTreePlain(points, 0, size - 1, 0);
    }
    else
    {
        DPU_LOG("buildOnChip: Using sketch-based tree build");
        root = buildTree(points, size, 0);
    }

    if(!root)
    {
        DPU_LOG("buildOnChip: ERR - buildTree failed");
        return NULL;
    }

    initializeCounters(root);

    KDGroup** groups = logStarDecompose(root, size);
    if(!groups)
    {
        DPU_LOG("buildOnChip: ERR - logStarDecompose failed");
        return NULL;
    }

    for(int i = 1; groups[i] != NULL; ++i)
    {
        DPU_LOG("buildOnChip: Building replicas for group %d", i);
        buildGroupReplicas(groups[i]);
    }

    return root;
}

KDNode* buildTree(point** points, size_t size, uint16_t depth)
{
    DPU_LOG("buildTree: size=%zu, depth=%u", size, depth);

    if(size <= getLeafWrapThreshold())
    {
        DPU_LOG("buildTree: Creating leaf node (size=%zu <= leafWrapThreshold=%u)", size, getLeafWrapThreshold());
        return createLeafNode(points, size);
    }

    size_t sampleCount = getChunkSize() * getOversamplingRate();

    point** samples = (point**)mem_alloc(sampleCount * sizeof(point*));
    if(!samples)
    {
        DPU_LOG("buildTree: ERR - mem_alloc failed for samples (%zu bytes)", sampleCount * sizeof(point*));
        return NULL;
    }

    setupRand();

    for(size_t i = 0; i < sampleCount; ++i)
        samples[i] = points[rand() % size];

    KDNode* sketch = NULL;
    buildSketch(&sketch, samples, sampleCount, getSketchHeight());

    if(!sketch)
    {
        DPU_LOG("buildTree: ERR - buildSketch failed");
        return NULL;
    }

    Bucket* buckets = sievePoints(points, size, sketch);
    if(!buckets)
    {
        DPU_LOG("buildTree: ERR - sievePoints failed");
        return NULL;
    }

    for(size_t i = 0; i < getChunkSize(); ++i)
    {
        if(buckets[i].size > 0)
        {
            KDNode* subtree = buildTree(buckets[i].points, buckets[i].size, depth + getSketchHeight());
            attachSubtree(sketch, i, subtree);
        }
    }

    return sketch;
}

KDNode* buildTreePlain(point** points, size_t start, size_t end, uint16_t depth)
{
    size_t size = end - start + 1;

    if(size <= getLeafWrapThreshold())
    {
        DPU_LOG("buildTreePlain: Creating leaf node");
        return createLeafNode(points + start, size);
    }

    KDNode* node = (KDNode*)mem_alloc(sizeof(KDNode));
    if(!node)
    {
        DPU_LOG("buildTreePlain: ERR - mem_alloc failed for KDNode");
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

    size_t mid = partitionPoints(points, start, end, splitDim, splitValue);

    node->data.internal.left = buildTreePlain(points, start, mid - 1, depth + 1);
    if(node->data.internal.left)
        node->data.internal.left->parent = node;

    node->data.internal.right = buildTreePlain(points, mid, end, depth + 1);
    if(node->data.internal.right)
        node->data.internal.right->parent = node;

    uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
    uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
    node->data.internal.approximateCounter = leftSize + rightSize;

    return node;
}

KDNode* createLeafNode(point** points, size_t size)
{
    KDNode* leaf = (KDNode*)mem_alloc(sizeof(KDNode));
    if(!leaf)
    {
        DPU_LOG("createLeafNode: ERR - mem_alloc failed for KDNode");
        return NULL;
    }

    leaf->type = LEAF;
    leaf->parent = NULL;
    leaf->data.leaf.pointsCount = size;

    if(size > 0)
    {
        leaf->data.leaf.points = (point*)mem_alloc(size * sizeof(point));
        if(!leaf->data.leaf.points)
        {
            DPU_LOG("createLeafNode: ERR - mem_alloc failed for points array (%zu bytes)", size * sizeof(point));
            return NULL;
        }

        for(size_t i = 0; i < size; ++i)
            leaf->data.leaf.points[i].coords = points[i]->coords;
    }
    else
        leaf->data.leaf.points = NULL;

    return leaf;
}

void buildSketch(KDNode** root, point** points, size_t sampleCount, uint16_t levels)
{
    *root = (KDNode*)mem_alloc(sizeof(KDNode));
    if(!*root)
    {
        DPU_LOG("buildSketch: ERR - mem_alloc failed for root");
        return;
    }

    if(levels == 0 || sampleCount <= getLeafWrapThreshold())
    {
        DPU_LOG("buildSketch: Creating leaf sketch (levels=%u, sampleCount=%zu <= threshold=%u)", levels, sampleCount, getLeafWrapThreshold());
        (*root)->type = LEAF;
        (*root)->parent = NULL;
        (*root)->data.leaf.points = NULL;
        (*root)->data.leaf.pointsCount = 0;
        return;
    }

    uint8_t splitDim = findSplitDim(points, 0, sampleCount - 1);
    float splitValue = findMedian(points, 0, sampleCount - 1, splitDim);

    (*root)->type = INTERNAL;
    (*root)->data.internal.splitDim = splitDim;
    (*root)->data.internal.splitValue = splitValue;
    (*root)->data.internal.approximateCounter = 0;
    (*root)->data.internal.left = NULL;
    (*root)->data.internal.right = NULL;
    (*root)->parent = NULL;

    size_t leftCount = 0;
    size_t rightCount = 0;

    point** leftSamples = (point**)mem_alloc(sampleCount * sizeof(point*));
    point** rightSamples = (point**)mem_alloc(sampleCount * sizeof(point*));

    if(!leftSamples || !rightSamples)
    {
        DPU_LOG("buildSketch: ERR - mem_alloc failed for sample arrays");
        *root = NULL;
        return;
    }

    for(size_t i = 0; i < sampleCount; ++i)
    {
        if(points[i]->coords[splitDim] < splitValue)
            leftSamples[leftCount++] = points[i];
        else
            rightSamples[rightCount++] = points[i];
    }

    buildSketch(&(*root)->data.internal.left, leftSamples, leftCount, levels - 1);
    buildSketch(&(*root)->data.internal.right, rightSamples, rightCount, levels - 1);

    if((leftCount > 0 && !(*root)->data.internal.left) || (rightCount > 0 && !(*root)->data.internal.right))
    {
        DPU_LOG("buildSketch: ERR - Child sketch creation failed");
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
}

Bucket* sievePoints(point** points, size_t size, KDNode* sketch)
{
    uint16_t chunkSize = getChunkSize();

    uint32_t* bucketCounts = (uint32_t*)mem_alloc(chunkSize * sizeof(uint32_t));
    if(!bucketCounts)
    {
        DPU_LOG("sievePoints: ERR - mem_alloc failed for bucketCounts");
        return NULL;
    }

    for(size_t i = 0; i < chunkSize; ++i)
        bucketCounts[i] = 0;

    for(size_t i = 0; i < size; ++i)
    {
        uint32_t bucketId = getBucket(sketch, points[i]->coords);
        if(bucketId < chunkSize)
            ++bucketCounts[bucketId];
    }

    uint32_t* offsets = (uint32_t*)mem_alloc((chunkSize + 1) * sizeof(uint32_t));
    if(!offsets)
    {
        DPU_LOG("sievePoints: ERR - mem_alloc failed for offsets");
        return NULL;
    }

    offsets[0] = 0;
    for(size_t j = 0; j < chunkSize; ++j)
        offsets[j + 1] = offsets[j] + bucketCounts[j];

    point** sortedPoints = (point**)mem_alloc(size * sizeof(point*));
    if(!sortedPoints)
    {
        DPU_LOG("sievePoints: ERR - mem_alloc failed for sortedPoints");
        return NULL;
    }

    for(size_t i = 0; i < size; ++i)
        sortedPoints[i] = points[i];

    uint32_t* currentPos = (uint32_t*)mem_alloc(chunkSize * sizeof(uint32_t));
    if(!currentPos)
    {
        DPU_LOG("sievePoints: ERR - mem_alloc failed for currentPos");
        return NULL;
    }

    for(size_t j = 0; j < chunkSize; ++j)
        currentPos[j] = offsets[j];

    point** tempPoints = (point**)mem_alloc(size * sizeof(point*));
    if(!tempPoints)
    {
        DPU_LOG("sievePoints: ERR - mem_alloc failed for tempPoints");
        return NULL;
    }

    for(size_t i = 0; i < size; ++i)
    {
        uint32_t bucketId = getBucket(sketch, sortedPoints[i]->coords);
        if(bucketId >= chunkSize)
            bucketId = 0;

        uint32_t pos = currentPos[bucketId]++;
        tempPoints[pos] = sortedPoints[i];
    }

    for(size_t i = 0; i < size; ++i)
        points[i] = tempPoints[i];

    Bucket* buckets = (Bucket*)mem_alloc(chunkSize * sizeof(Bucket));
    if(!buckets)
    {
        DPU_LOG("sievePoints: ERR - mem_alloc failed for buckets");
        return NULL;
    }

    for(size_t j = 0; j < chunkSize; ++j)
    {
        buckets[j].points = &points[offsets[j]];
        buckets[j].size = bucketCounts[j];
    }

    return buckets;
}

void attachSubtree(KDNode* sketch, uint16_t bucketId, KDNode* subtree)
{
    if(!sketch || !subtree || bucketId >= getChunkSize())
    {
        DPU_LOG("attachSubtree: ERR - Invalid parameters");
        return;
    }

    KDNode* current = sketch;
    uint16_t sketchHeight = getSketchHeight();
    uint16_t level = 0;

    for(level = 0; level < sketchHeight - 1; ++level)
    {
        uint16_t shift = sketchHeight - 1 - level;
        uint8_t bit = (bucketId >> shift) & 1;

        if(bit == 0)
        {
            if(!current->data.internal.left)
                break;
            current = current->data.internal.left;
        }
        else
        {
            if(!current->data.internal.right)
                break;
            current = current->data.internal.right;
        }
    }

    while(current && current->type == INTERNAL && level < sketchHeight)
    {
        uint8_t bit = (bucketId >> (sketchHeight - 1 - level)) & 1;
        if(bit == 0)
        {
            if(!current->data.internal.left)
            {
                KDNode* tempLeaf = (KDNode*)mem_alloc(sizeof(KDNode));
                if(!tempLeaf)
                {
                    DPU_LOG("attachSubtree: ERR - mem_alloc failed for tempLeaf");
                    break;
                }

                tempLeaf->type = LEAF;
                tempLeaf->parent = current;
                tempLeaf->data.leaf.points = NULL;
                tempLeaf->data.leaf.pointsCount = 0;
                current->data.internal.left = tempLeaf;
            }

            current = current->data.internal.left;
        }
        else
        {
            if(!current->data.internal.right)
            {
                KDNode* tempLeaf = (KDNode*)mem_alloc(sizeof(KDNode));
                if(!tempLeaf)
                {
                    DPU_LOG("attachSubtree: ERR - mem_alloc failed for tempLeaf");
                    break;
                }

                tempLeaf->type = LEAF;
                tempLeaf->parent = current;
                tempLeaf->data.leaf.points = NULL;
                tempLeaf->data.leaf.pointsCount = 0;
                current->data.internal.right = tempLeaf;
            }

            current = current->data.internal.right;
        }

        ++level;
    }

    if(!current || current->type != LEAF)
    {
        DPU_LOG("attachSubtree: ERR - Invalid target node");
        return;
    }

    if(current->type == LEAF)
    {
        current->type = INTERNAL;
        current->data.internal.splitDim = 0;
        current->data.internal.splitValue = 0.0f;
        current->data.internal.approximateCounter = 0;
        current->data.internal.left = subtree;
        current->data.internal.right = NULL;

        if(subtree)
            subtree->parent = current;

        current->data.leaf.points = NULL;
        current->data.leaf.pointsCount = 0;
    }

    KDNode* updateNode = current;
    while(updateNode)
    {
        if(updateNode->type == INTERNAL)
        {
            uint32_t leftSize = updateNode->data.internal.left ? getNodeSize(updateNode->data.internal.left) : 0;
            uint32_t rightSize = updateNode->data.internal.right ? getNodeSize(updateNode->data.internal.right) : 0;
            updateNode->data.internal.approximateCounter = leftSize + rightSize;
        }

        updateNode = updateNode->parent;
    }
}

KDGroup** logStarDecompose(KDNode* root, size_t totalPoints)
{
    if(!root)
    {
        DPU_LOG("logStarDecompose: ERR - root is NULL");
        return NULL;
    }

    size_t P = getNPim();
    uint8_t numGroups = 0;

    while(P > 1)
    {
        ++numGroups;
        P = (size_t)log2f(P);
    }

    ++numGroups;
    P = getNPim();

    KDGroup** groups = (KDGroup**)mem_alloc((numGroups + 1) * sizeof(KDGroup*));
    if(!groups)
    {
        DPU_LOG("logStarDecompose: ERR - mem_alloc failed for groups");
        return NULL;
    }

    size_t* H = (size_t*)mem_alloc((numGroups + 1) * sizeof(size_t));
    if(!H)
    {
        DPU_LOG("logStarDecompose: ERR - mem_alloc failed for H array");
        return NULL;
    }

    H[0] = P;
    for(uint8_t j = 1; j <= numGroups; ++j)
    {
        H[j] = (size_t)log2f(H[j - 1]);
        if(H[j] < 1)
            H[j] = 1;
    }

    for(uint8_t j = 0; j < numGroups; ++j)
    {
        groups[j] = (KDGroup*)mem_alloc(sizeof(KDGroup));
        if(!groups[j])
        {
            DPU_LOG("logStarDecompose: ERR - mem_alloc failed for group %d", j);
            return NULL;
        }

        groups[j]->masterRoots = NULL;
        groups[j]->masterRootCount = 0;

        if(j == 0)
        {
            groups[j]->minSize = H[0];
            groups[j]->maxSize = FLT_MAX;
        }
        else
        {
            groups[j]->minSize = H[j];
            groups[j]->maxSize = H[j - 1];
        }
    }

    groups[numGroups] = NULL;
    assignNodesToGroups(root, groups, numGroups);

    return groups;
}

void assignNodesToGroups(KDNode* node, KDGroup** groups, uint8_t numGroups)
{
    if(!node || !groups)
        return;

    size_t subtreeSize = getNodeSize(node);
    int16_t groupId = findGroup(subtreeSize, groups, numGroups);

    if(groupId >= 0 && groupId < numGroups && groups[groupId])
    {
        if(groups[groupId]->masterRoots == NULL)
        {
            groups[groupId]->masterRoots = (KDNode**)mem_alloc(32 * sizeof(KDNode*));
            if(!groups[groupId]->masterRoots)
            {
                DPU_LOG("assignNodesToGroups: ERR - mem_alloc failed for masterRoots");
                return;
            }
        }

        if(groups[groupId]->masterRootCount < 32)
            groups[groupId]->masterRoots[groups[groupId]->masterRootCount++] = node;
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

        if(src->data.leaf.pointsCount > 0)
        {
            dest->data.leaf.points = (point*)mem_alloc(src->data.leaf.pointsCount * sizeof(point));

            if(dest->data.leaf.points)
                for(size_t i = 0; i < src->data.leaf.pointsCount; ++i)
                    dest->data.leaf.points[i].coords = src->data.leaf.points[i].coords;
        }
        else
            dest->data.leaf.points = NULL;
    }
}

KDNodeReplica* createReplicaFromMaster(KDNode* masterNode)
{
    if(!masterNode)
    {
        DPU_LOG("createReplicaFromMaster: ERR - masterNode is NULL");
        return NULL;
    }


    KDNodeReplica* replica = (KDNodeReplica*)mem_alloc(sizeof(KDNodeReplica));
    if(!replica)
    {
        DPU_LOG("createReplicaFromMaster: ERR - mem_alloc failed for replica");
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
    }
    else
    {
        replica->data.leaf.pointsCount = masterNode->data.leaf.pointsCount;

        if(masterNode->data.leaf.pointsCount > 0)
        {
            replica->data.leaf.points = (point*)mem_alloc(masterNode->data.leaf.pointsCount * sizeof(point));

            if(replica->data.leaf.points)
                for(size_t i = 0; i < masterNode->data.leaf.pointsCount; ++i)
                    replica->data.leaf.points[i].coords = masterNode->data.leaf.points[i].coords;
        }
        else
            replica->data.leaf.points = NULL;
    }

    return replica;
}

void buildTopDownReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica)
{
    if(!masterNode || !group || !replica)
        return;

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

    if(leftInGroup && leftChild)
    {
        KDNodeReplica* leftReplica = createReplicaFromMaster(leftChild);
        if(leftReplica)
        {
            replica->data.internal.left = leftReplica;
            leftReplica->parent = replica;

            buildTopDownReplica(leftChild, group, leftReplica);

            if(replica->descendants == NULL)
                replica->descendants = (KDNodeReplica**)mem_alloc(2 * sizeof(KDNodeReplica*));

            if(replica->descendants && replica->descendantCount < 2)
                replica->descendants[replica->descendantCount++] = leftReplica;
        }
    }

    if(rightInGroup && rightChild)
    {
        KDNodeReplica* rightReplica = createReplicaFromMaster(rightChild);
        if(rightReplica)
        {
            replica->data.internal.right = rightReplica;
            rightReplica->parent = replica;

            buildTopDownReplica(rightChild, group, rightReplica);

            if(replica->descendants == NULL)
                replica->descendants = (KDNodeReplica**)mem_alloc(2 * sizeof(KDNodeReplica*));

            if(replica->descendants && replica->descendantCount < 2)
                replica->descendants[replica->descendantCount++] = rightReplica;
        }
    }
}

void buildBottomUpReplica(KDNode* masterNode, KDGroup* group, KDNodeReplica* replica)
{
    if(!masterNode || !group || !replica)
        return;

    KDNode* currentAncestor = masterNode->parent;
    KDNodeReplica* currentReplica = replica;

    while(currentAncestor)
    {
        size_t ancestorSize = getNodeSize(currentAncestor);

        if(ancestorSize >= group->minSize && ancestorSize < group->maxSize)
        {
            KDNodeReplica* ancestorReplica = createReplicaFromMaster(currentAncestor);
            if(!ancestorReplica)
                break;

            if(currentAncestor->data.internal.left == currentReplica->masterNode)
                ancestorReplica->data.internal.left = currentReplica;
            else if(currentAncestor->data.internal.right == currentReplica->masterNode)
                ancestorReplica->data.internal.right = currentReplica;

            currentReplica->parent = ancestorReplica;

            if(replica->ancestors == NULL)
                replica->ancestors = (KDNodeReplica**)mem_alloc(8 * sizeof(KDNodeReplica*));

            if(replica->ancestors && replica->ancestorCount < 8)
                replica->ancestors[replica->ancestorCount++] = ancestorReplica;

            currentReplica = ancestorReplica;
            currentAncestor = currentAncestor->parent;
        }
        else
            break;
    }
}

void buildGroupReplicas(KDGroup* group)
{
    if(!group || group->masterRootCount == 0)
    {
        DPU_LOG("buildGroupReplicas: Skipping - no master roots");
        return;
    }

    group->replicas = (KDNodeReplica**)mem_alloc(group->masterRootCount * sizeof(KDNodeReplica*));

    if(!group->replicas)
    {
        DPU_LOG("buildGroupReplicas: ERR - mem_alloc failed for replicas array");
        return;
    }

    group->replicaCount = 0;

    for(size_t i = 0; i < group->masterRootCount; ++i)
    {
        KDNode* masterNode = group->masterRoots[i];

        KDNodeReplica* replica = createReplicaFromMaster(masterNode);
        if(!replica)
            continue;

        buildTopDownReplica(masterNode, group, replica);
        buildBottomUpReplica(masterNode, group, replica);

        group->replicas[group->replicaCount++] = replica;
    }
}

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <alloc.h>
#include <stdio.h>

#include "dpu/kdTree/serialization.h"
#include "dpu/kdTree/types.h"
#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"

size_t calculateSerializedSize(KDNode* node)
{
    if(!node)
    {
        DPU_LOG("calculateSerializedSize: node is NULL");
        return 0;
    }

    uint32_t dims = getDimensions();
    size_t size = sizeof(uint8_t);

    if(node->type == INTERNAL)
    {
        size += sizeof(uint32_t);
        size += sizeof(uint8_t);
        size += sizeof(float);

        size += calculateSerializedSize(node->data.internal.left);
        size += calculateSerializedSize(node->data.internal.right);
    }
    else
    {
        size += sizeof(uint32_t);
        size += node->data.leaf.pointsCount * dims * sizeof(float);
    }

    return size;
}

void serializeNode(KDNode* node, uint8_t** ptr)
{
    if(!node)
    {
        DPU_LOG("serializeNode: node is NULL");
        return;
    }

    uint32_t dims = getDimensions();

    **ptr = (node->type == INTERNAL) ? 0 : 1;
    ++(*ptr);

    if(node->type == INTERNAL)
    {
        memcpy(*ptr, &node->data.internal.approximateCounter, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        **ptr = node->data.internal.splitDim;
        ++(*ptr);

        memcpy(*ptr, &node->data.internal.splitValue, sizeof(float));
        (*ptr) += sizeof(float);

        serializeNode(node->data.internal.left, ptr);
        serializeNode(node->data.internal.right, ptr);
    }
    else
    {
        uint32_t count = node->data.leaf.pointsCount;
        memcpy(*ptr, &count, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        if(count > 0 && node->data.leaf.points)
        {
            for(size_t i = 0; i < count; ++i)
            {
                memcpy(*ptr, node->data.leaf.points[i].coords, dims * sizeof(float));
                (*ptr) += dims * sizeof(float);
            }
        }
    }
}

uint8_t* serializeTree(KDNode* root, size_t* size)
{
    if(!root || !size)
    {
        DPU_LOG("serializeTree: ERR - Invalid parameters");
        return NULL;
    }

    *size = calculateSerializedSize(root);

    uint8_t* buffer = (uint8_t*)mem_alloc(*size);
    if(!buffer)
    {
        DPU_LOG("serializeTree: ERR - mem_alloc failed for buffer (%zu bytes)", *size);
        return NULL;
    }

    uint8_t* ptr = buffer;
    serializeNode(root, &ptr);

    return buffer;
}

size_t calculateReplicaSerializedSize(KDNodeReplica* replica)
{
    if(!replica)
    {
        DPU_LOG("calculateReplicaSerializedSize: replica is NULL");
        return 0;
    }

    uint32_t dims = getDimensions();
    size_t size = 1;

    size += sizeof(uint64_t);
    size += sizeof(size_t);
    size += sizeof(size_t);

    if(replica->type == INTERNAL)
    {
        size += sizeof(uint32_t);
        size += 1 + sizeof(float);
        size += calculateReplicaSerializedSize(replica->data.internal.left);
        size += calculateReplicaSerializedSize(replica->data.internal.right);
    }
    else
    {
        size += sizeof(uint32_t);
        size += replica->data.leaf.pointsCount * dims * sizeof(float);
    }

    return size;
}

void serializeReplicaNode(KDNodeReplica* replica, uint8_t** ptr)
{
    if(!replica)
    {
        DPU_LOG("serializeReplicaNode: replica is NULL");
        return;
    }

    uint32_t dims = getDimensions();

    **ptr = (replica->type == INTERNAL) ? 0 : 1;
    ++(*ptr);

    uint64_t masterAddr = (uint64_t)(uintptr_t)replica->masterNode;
    memcpy(*ptr, &masterAddr, sizeof(uint64_t));
    (*ptr) += sizeof(uint64_t);

    memcpy(*ptr, &replica->descendantCount, sizeof(size_t));
    (*ptr) += sizeof(size_t);
    memcpy(*ptr, &replica->ancestorCount, sizeof(size_t));
    (*ptr) += sizeof(size_t);

    if(replica->type == INTERNAL && replica->data.internal.left && replica->data.internal.left->masterNode)
    {
        memcpy(*ptr, &replica->data.internal.left->masterNode->data.internal.approximateCounter, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        **ptr = replica->data.internal.left->masterNode->data.internal.splitDim;
        ++(*ptr);

        memcpy(*ptr, &replica->data.internal.left->masterNode->data.internal.splitValue, sizeof(float));
        (*ptr) += sizeof(float);

        serializeReplicaNode(replica->data.internal.left, ptr);
        serializeReplicaNode(replica->data.internal.right, ptr);
    }
    else
    {
        uint32_t count = replica->data.leaf.pointsCount;
        memcpy(*ptr, &count, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        if(count > 0 && replica->data.leaf.points)
        {
            for(size_t i = 0; i < count; ++i)
            {
                memcpy(*ptr, replica->data.leaf.points[i].coords, dims * sizeof(float));
                (*ptr) += dims * sizeof(float);
            }
        }
    }
}

uint8_t* serializeReplicaTree(KDNodeReplica* root, size_t* size)
{
    if(!root || !size)
    {
        DPU_LOG("serializeReplicaTree: ERR - Invalid parameters");
        return NULL;
    }

    *size = calculateReplicaSerializedSize(root);

    uint8_t* buffer = (uint8_t*)mem_alloc(*size);
    if(!buffer)
    {
        DPU_LOG("serializeReplicaTree: ERR - mem_alloc failed for buffer (%zu bytes)", *size);
        return NULL;
    }

    uint8_t* ptr = buffer;
    serializeReplicaNode(root, &ptr);

    return buffer;
}

size_t calculateFullSerializedSize(KDNode* root, KDGroup** groups)
{
    size_t total = calculateSerializedSize(root);

    for(int i = 1; groups[i] != NULL; ++i)
    {
        for(size_t j = 0; j < groups[i]->replicaCount; ++j)
        {
            size_t replicaSize = calculateReplicaSerializedSize(groups[i]->replicas[j]);
            total += replicaSize;
        }
    }

    return total;
}

uint8_t* serializeFullTree(KDNode* root, KDGroup** groups, size_t* size)
{
    if(!root || !size)
    {
        DPU_LOG("serializeFullTree: ERR - Invalid parameters");
        return NULL;
    }

    *size = calculateFullSerializedSize(root, groups);

    uint8_t* buffer = (uint8_t*)mem_alloc(*size);
    if(!buffer)
    {
        DPU_LOG("serializeFullTree: ERR - mem_alloc failed for buffer (%zu bytes)", *size);
        return NULL;
    }

    uint8_t* ptr = buffer;

    serializeNode(root, &ptr);

    for(int i = 1; groups[i] != NULL; ++i)
        for(size_t j = 0; j < groups[i]->replicaCount; ++j)
            serializeReplicaNode(groups[i]->replicas[j], &ptr);

    return buffer;
}

KDNodeReplica* deserializeReplicaTree(uint8_t* buffer, size_t size)
{
    if(!buffer || size == 0)
    {
        DPU_LOG("deserializeReplicaTree: ERR - Invalid parameters");
        return NULL;
    }

    uint8_t* ptr = buffer;
    KDNodeReplica* result = deserializeReplicaNode(&ptr);

    if(result)
        DPU_LOG("deserializeReplicaTree: Deserialization completed successfully");
    else
        DPU_LOG("deserializeReplicaTree: ERR - Deserialization failed");

    return result;
}

KDNodeReplica* deserializeReplicaNode(uint8_t** ptr)
{
    if(!ptr || !*ptr)
    {
        DPU_LOG("deserializeReplicaNode: ERR - Invalid pointer");
        return NULL;
    }

    uint8_t type = **ptr;
    ++(*ptr);

    KDNodeReplica* replica = (KDNodeReplica*)mem_alloc(sizeof(KDNodeReplica));
    if(!replica)
    {
        DPU_LOG("deserializeReplicaNode: ERR - mem_alloc failed for replica");
        return NULL;
    }

    replica->type = (type == 0) ? INTERNAL : LEAF;

    uint64_t masterAddr;
    memcpy(&masterAddr, *ptr, sizeof(uint64_t));
    (*ptr) += sizeof(uint64_t);
    replica->masterNode = (KDNode*)(uintptr_t)masterAddr;

    memcpy(&replica->descendantCount, *ptr, sizeof(size_t));
    (*ptr) += sizeof(size_t);

    memcpy(&replica->ancestorCount, *ptr, sizeof(size_t));
    (*ptr) += sizeof(size_t);

    replica->parent = NULL;
    replica->descendants = NULL;
    replica->ancestors = NULL;

    if(replica->type == INTERNAL)
    {
        uint32_t counter;
        memcpy(&counter, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        uint8_t splitDim = **ptr;
        ++(*ptr);

        float splitValue;
        memcpy(&splitValue, *ptr, sizeof(float));
        (*ptr) += sizeof(float);

        replica->data.internal.left = deserializeReplicaNode(ptr);

        replica->data.internal.right = deserializeReplicaNode(ptr);

        if(replica->data.internal.left)
            replica->data.internal.left->parent = replica;

        if(replica->data.internal.right)
            replica->data.internal.right->parent = replica;
    }
    else
    {
        uint32_t dims = getDimensions();
        uint32_t pointsCount;
        memcpy(&pointsCount, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        replica->data.leaf.pointsCount = pointsCount;

        if(pointsCount > 0)
        {
            replica->data.leaf.points = (point*)mem_alloc(pointsCount * sizeof(point));

            if(replica->data.leaf.points)
            {
                for(size_t i = 0; i < pointsCount; ++i)
                {
                    replica->data.leaf.points[i].coords = (float*)mem_alloc(dims * sizeof(float));
                    if(replica->data.leaf.points[i].coords)
                    {
                        memcpy(replica->data.leaf.points[i].coords, *ptr, dims * sizeof(float));
                        (*ptr) += dims * sizeof(float);
                    }
                }
            }
        }
        else
            replica->data.leaf.points = NULL;
    }

    return replica;
}

KDNode* deserializeTree(uint8_t* buffer, size_t size)
{
    if(!buffer || size == 0)
    {
        DPU_LOG("deserializeTree: ERR - Invalid parameters");
        return NULL;
    }

    uint8_t* ptr = buffer;
    uint8_t* end = ptr + size;

    KDNode* result = deserializeNode(&ptr, end);

    if(result)
        DPU_LOG("deserializeTree: Deserialization completed successfully");
    else
        DPU_LOG("deserializeTree: ERR - Deserialization failed");

    return result;
}

KDNode* deserializeNode(uint8_t** ptr, uint8_t* end)
{
    if(!ptr || !*ptr || *ptr >= end)
    {
        DPU_LOG("deserializeNode: ERR - Invalid pointer or buffer end");
        return NULL;
    }

    uint8_t type = **ptr;
    ++(*ptr);

    KDNode* node = (KDNode*)mem_alloc(sizeof(KDNode));
    if(!node)
    {
        DPU_LOG("deserializeNode: ERR - mem_alloc failed for node");
        return NULL;
    }

    node->type = (type == 0) ? INTERNAL : LEAF;
    node->parent = NULL;

    if(node->type == INTERNAL)
    {
        memcpy(&node->data.internal.approximateCounter, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        node->data.internal.splitDim = **ptr;
        ++(*ptr);

        memcpy(&node->data.internal.splitValue, *ptr, sizeof(float));
        (*ptr) += sizeof(float);

        node->data.internal.left = NULL;
        node->data.internal.right = NULL;

        node->data.internal.left = deserializeNode(ptr, end);
        if(node->data.internal.left)
            node->data.internal.left->parent = node;

        node->data.internal.right = deserializeNode(ptr, end);
        if(node->data.internal.right)
            node->data.internal.right->parent = node;
    }
    else
    {
        uint32_t dims = getDimensions();
        uint32_t pointsCount;
        memcpy(&pointsCount, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        node->data.leaf.pointsCount = pointsCount;

        if(pointsCount > 0)
        {
            node->data.leaf.points = (point*)mem_alloc(pointsCount * sizeof(point));

            if(node->data.leaf.points)
            {
                for(size_t i = 0; i < pointsCount; ++i)
                {
                    node->data.leaf.points[i].coords = (float*)mem_alloc(dims * sizeof(float));
                    if(node->data.leaf.points[i].coords)
                    {
                        memcpy(node->data.leaf.points[i].coords, *ptr, dims * sizeof(float));
                        (*ptr) += dims * sizeof(float);
                    }
                }
            }
        }
        else
            node->data.leaf.points = NULL;
    }

    return node;
}

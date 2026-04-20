#include <stdlib.h>
#include <string.h>

#include "host/kdTree/types.h"
#include "host/kdTree/serialization.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/distribute.h"

#include "host/environment/init.h"
#include "host/management/logging.h"

void* serializeTree(KDNode* root, size_t* size)
{
    logMessage("Serializing KD-tree", DEBUG);

    if(!root || !size)
    {
        logMessage("Null root or size pointer provided", ERROR);
        return NULL;
    }

    size_t totalSize = 0;
    serializeNodeSize(root, &totalSize);

    void* buffer = malloc(totalSize);
    if(!buffer)
    {
        logMessage("Failed to allocate serialization buffer", ERROR);
        return NULL;
    }

    uint8_t* ptr = (uint8_t*)buffer;
    serializeNodeData(root, &ptr);

    *size = totalSize;
    logMessage("KD-tree serialized", INFO);
    return buffer;
}

void serializeNodeSize(KDNode* node, size_t* size)
{
    if(!node)
    {
        logMessage("Null node encountered while computing serialization size", DEBUG);
        return;
    }

    *size += 1;

    if(node->type == INTERNAL)
    {
        logMessage("Processing internal node for size computation", DEBUG);

        *size += sizeof(uint32_t);
        *size += 1 + sizeof(float);

        serializeNodeSize(node->data.internal.left, size);
        serializeNodeSize(node->data.internal.right, size);
    }
    else
    {
        logMessage("Processing leaf node for size computation", DEBUG);

        *size += sizeof(uint32_t);

        if(node->data.leaf.pointsCount > 0 && node->data.leaf.points)
        {
            size_t added = node->data.leaf.pointsCount * getDimensions() * sizeof(float);
            *size += added;

            char msg[128];
            snprintf(msg, sizeof(msg), "Leaf contributes %zu bytes", added);
            logMessage(msg, INFO);
        }
    }
}

void serializeNodeData(KDNode* node, uint8_t** ptr)
{
    if(!node)
    {
        logMessage("Null node encountered during serialization", DEBUG);
        return;
    }

    **ptr = (node->type == INTERNAL) ? 0 : 1;
    ++(*ptr);

    if(node->type == INTERNAL)
    {
        logMessage("Serializing internal node", DEBUG);

        memcpy(*ptr, &node->data.internal.approximateCounter, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        **ptr = node->data.internal.splitDim;
        ++(*ptr);

        memcpy(*ptr, &node->data.internal.splitValue, sizeof(float));
        *ptr += sizeof(float);

        serializeNodeData(node->data.internal.left, ptr);
        serializeNodeData(node->data.internal.right, ptr);
    }
    else
    {
        logMessage("Serializing leaf node", DEBUG);

        uint32_t count = (uint32_t)node->data.leaf.pointsCount;
        memcpy(*ptr, &count, sizeof(uint32_t));
        *ptr += sizeof(uint32_t);

        if(count > 0 && node->data.leaf.points)
        {
            for(size_t i = 0; i < count; i++)
            {
                memcpy(*ptr, node->data.leaf.points[i].coords, getDimensions() * sizeof(float));

                *ptr += getDimensions() * sizeof(float);
            }

            char msg[128];
            snprintf(msg, sizeof(msg), "Serialized %u points in leaf", count);
            logMessage(msg, INFO);
        }
        else
        {
            logMessage("Leaf node contains no points", DEBUG);
        }
    }
}

KDNode* deserializeTree(void* data, size_t size)
{
    logMessage("Deserializing KD-tree", DEBUG);

    if(!data || size == 0)
    {
        logMessage("Null data or zero size provided", ERROR);
        return NULL;
    }

    uint8_t* ptr = (uint8_t*)data;
    uint8_t* end = ptr + size;

    KDNode* root = deserializeNode(&ptr, end);

    if(!root)
        logMessage("KD-tree deserialization failed", ERROR);
    else
        logMessage("KD-tree deserialized", INFO);

    return root;
}

KDNode* deserializeNode(uint8_t** ptr, uint8_t* end)
{
    if(!ptr || !*ptr || *ptr >= end)
    {
        logMessage("Deserialization pointer out of bounds", ERROR);
        return NULL;
    }


    if(end - *ptr < 1)
    {
        logMessage("Not enough bytes for node type", ERROR);
        return NULL;
    }

    uint8_t type = **ptr;
    ++(*ptr);

    KDNode* node = malloc(sizeof(KDNode));
    if(!node) return NULL;

    node->type = (type == 0) ? INTERNAL : LEAF;
    node->parent = NULL;

    if(node->type == INTERNAL)
    {
        if(end - *ptr < sizeof(uint32_t))
        {
            logMessage("Not enough bytes for approximateCounter", ERROR);
            free(node);
            return NULL;
        }
        memcpy(&node->data.internal.approximateCounter, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);

        if(end - *ptr < 1)
        {
            logMessage("Not enough bytes for splitDim", ERROR);
            free(node);
            return NULL;
        }

        node->data.internal.splitDim = **ptr;
        ++(*ptr);

        if(end - *ptr < sizeof(float))
        {
            logMessage("Not enough bytes for splitValue", ERROR);
            free(node);
            return NULL;
        }

        memcpy(&node->data.internal.splitValue, *ptr, sizeof(float));
        (*ptr) += sizeof(float);

        node->data.internal.left = deserializeNode(ptr, end);
        node->data.internal.right = deserializeNode(ptr, end);
    }
    else
    {
        if(end - *ptr < sizeof(uint32_t))
        {
            logMessage("Not enough bytes for pointsCount", ERROR);
            free(node);
            return NULL;
        }

        memcpy(&node->data.leaf.pointsCount, *ptr, sizeof(uint32_t));
        (*ptr) += sizeof(uint32_t);
    }
    return node;
}

void serializeReplicaNode(KDNodeReplica* replica, uint8_t** ptr)
{
    if(!replica)
        return;

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
        return NULL;

    *size = calculateReplicaSerializedSize(root);
    uint8_t* buffer = (uint8_t*)malloc(*size);
    if(!buffer)
        return NULL;

    uint8_t* ptr = buffer;
    serializeReplicaNode(root, &ptr);

    return buffer;
}

size_t calculateReplicaSerializedSize(KDNodeReplica* replica)
{
    if(!replica)
        return 0;

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

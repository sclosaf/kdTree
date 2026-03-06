#include <stdlib.h>
#include <string.h>

#include "kdTree/serialize.h"

#include "environment/init.h"

void* serializeTree(KDNode* root, size_t* size)
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

void serializeNodeSize(KDNode* node, size_t* size)
{
    if(!node)
        return;

    *size += 1;

    if(node->type == INTERNAL)
    {
        *size += sizeof(uint32_t);
        *size += 1 + sizeof(float);

        serializeNodeSize(node->data.internal.left, size);
        serializeNodeSize(node->data.internal.right, size);
    }
    else
    {
        *size += sizeof(uint32_t);

        if(node->data.leaf.pointsCount > 0 && node->data.leaf.points)
            *size += node->data.leaf.pointsCount * getConfig()->dimensions * sizeof(float);
    }
}

void serializeNodeData(KDNode* node, uint8_t** ptr)
{
    if(!node)
        return;

    **ptr = (node->type == INTERNAL) ? 0 : 1;
    ++(*ptr);

    if(node->type == INTERNAL)
    {
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
        uint32_t count = (uint32_t)node->data.leaf.pointsCount;
        memcpy(*ptr, &count, sizeof(uint32_t));
        *ptr += sizeof(uint32_t);

        if(count > 0 && node->data.leaf.points)
        {
            for(size_t i = 0; i < count; i++)
            {
                memcpy(*ptr, node->data.leaf.points[i].coords, getConfig()->dimensions * sizeof(float));
                *ptr += getConfig()->dimensions * sizeof(float);
            }
        }
    }
}

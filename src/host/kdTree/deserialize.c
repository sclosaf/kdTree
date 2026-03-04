#include <stdlib.h>
#include <string.h>

#include "environment/init.h"

#include "kdTree/deserialize.h"

KDNode* deserializeTree(void* data, size_t size)
{
    if(!data || size == 0)
        return NULL;

    uint8_t* ptr = (uint8_t*)data;
    uint8_t* end = ptr + size;

    return deserializeNode(&ptr, end);
}

KDNode* deserializeNode(uint8_t** ptr, uint8_t* end)
{
    if(*ptr >= end)
        return NULL;

    KDNode* node = malloc(sizeof(KDNode));
    if(!node)
        return NULL;

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
                memcpy(node->data.leaf.points[i].coords, *ptr, getConfig()->dimensions * sizeof(float));
                (*ptr) += DIMENSIONS * sizeof(float);
            }
        }
        else
            node->data.leaf.points = NULL;
    }

    return node;
}

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

    uint8_t type = **ptr;
    ++(*ptr);

    KDNode* node = malloc(sizeof(KDNode));
    if(!node)
        return NULL;

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
                node->data.leaf.points[i].coords = malloc(getConfig()->dimensions * sizeof(float));
                if(!node->data.leaf.points[i].coords)
                {
                    for(size_t j = 0; j < i; ++j)
                        free(node->data.leaf.points[j].coords);

                    free(node->data.leaf.points);
                    free(node);
                    return NULL;
                }

                memcpy(node->data.leaf.points[i].coords, *ptr, getConfig()->dimensions * sizeof(float));
                (*ptr) += getConfig()->dimensions * sizeof(float);
            }
        }
        else
            node->data.leaf.points = NULL;
    }

    return node;
}

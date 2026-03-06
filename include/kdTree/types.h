#ifndef KDTREE_TYPES_H
#define KDTREE_TYPES_H

#include <stdint.h>
#include <stddef.h>

#include "environment/constants.h"
#include "environment/init.h"

typedef enum nodeType
{
    INTERNAL,
    LEAF
} nodeType;

typedef struct point
{
    float* coords;
} point;

typedef struct KDNode
{
    nodeType type;
    struct KDNode* parent;

    union
    {
        struct
        {
            uint8_t splitDim;
            float splitValue;
            struct KDNode* left;
            struct KDNode* right;

            uint32_t approximateCounter;
        } internal;
        struct
        {
            point* points;
            size_t pointsCount;
        } leaf;
    } data;
} KDNode;

typedef struct KDGroup
{
    KDNode** rootNodes;
    size_t count;

    float minSize;
    float maxSize;
} KDGroup;

typedef struct KDTree
{
    KDNode* root;
    uint32_t totalPoints;
    uint16_t totalNodes;
} KDTree;

#endif

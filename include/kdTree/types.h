#ifndef KDTREE_TYPES_H
#define KDTREE_TYPES_H

#include <stdint.h>
#include <stddef.h>

#include "utils/constants.h"

typedef enum nodeType
{
    INTERNAL,
    LEAF
} nodeType;

// typedef struct KDApproxCounter
// {
//     uint32_t value;
//     uint32_t totalTreeSize;
//     float beta;
//     uint32_t lastUpdate;
// } approxCounter;

typedef struct point
{
    float coords[DIMENSIONS];
} point;

typedef struct KDNode
{
    nodeType type;
    struct KDNode* parent;
    // KDApproxCounter size;

    union
    {
        struct
        {
            uint8_t splitDim;
            float splitValue;
            struct KDNode* left;
            struct KDNode* right;
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
    size_t totalPoints;
    uint16_t totalNodes;
} KDTree;

#endif

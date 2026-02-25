#ifndef COMPONENTS_H
#define COMPONENTS_H

#include "utils/types.h"
#include "utils/constants.h"

typedef enum nodeType
{
    INTERNAL,
    LEAF
} nodeType;

typedef struct point
{
    f32 coords[DIMENSIONS];
} point;

// typedef struct KDApproxCounter
// {
//     u32 value;
//     u32 totalTreeSize;
//     f32 beta;
//     u32 lastUpdate;
// } approxCounter;

typedef struct KDNode
{
    nodeType type;
    struct KDNode* parent;
    // KDApproxCounter size;

    union
    {
        struct
        {
            u8 splitDim;
            f32 splitValue;

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

    f32 minSize;
    f32 maxSize;
} KDGroup;

typedef struct KDTree
{
    KDNode* root;

    size_t totalPoints;
    u16 totalNodes;
} KDTree;

#endif

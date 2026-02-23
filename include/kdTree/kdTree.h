#ifndef KDTREE_H
#define KDTREE_H

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

// typedef struct approxCounter
// {
//     u32 value;
//     u32 totalTreeSize;
//     f32 beta;
//     u32 lastUpdate;
// } approxCounter;

typedef struct KDGroup
{
    u32 id;
    KDNode* rootNodes;
    size_t count;

    f32 minSize;
    f32 maxSize;
} KDGroup;

typedef struct KDNode
{
    nodeType type;
    KDNode* parent;
    // approxCounter size;

    union
    {
        struct
        {
            i8 splitDim;
            f32 splitValue;

            KDNode* left;
            KDNode* right;
        } internal;
        struct
        {
            point* points;
            size_t pointsCount;
        } leaf;
    } data;
} KDNode;

typedef struct KDTree
{
    KDNode* root;

    size_t totalPoints;
    u16 totalNodes;
} KDTree;

#endif

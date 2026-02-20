#ifndef KDTREE_H
#define KDTREE_H

#include "rename.h"
#include "constants.h"

typedef struct
{
    f32 coords[DIM];
} point;

typedef struct
{
    u32 value;
    u32 totalTreeSize;
    f32 beta;
    u32 lastUpdate;
} approxCounter;

typedef struct
{
    struct KDNode* root;

    size_t totalPoints;
    size_t totalNodes;

    u32 dimensions;
} KDTree;

typedef struct
{
    i8 splitDim;
    i8 splitValue;

    struct KDNode* parent;
    struct approxCounter size;

    union
    {
        struct
        {
            struct KDNode* left;
            struct KDNode* right;
        } internal;
        struct
        {
            struct point** points;
            u32 pointCount;
        } leaf;
    } data;
} KDNode;

#endif

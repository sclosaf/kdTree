#ifndef KDTREE_H
#define KDTREE_H

// IDs?

typedef struct
{
    f32 coords[];
} point;

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

    struct KDNode* left;
    struct KDNode* right;
    struct KDNode* parent;

    struct approxCounter size;
} KDNode;

typedef struct
{
    point** points;
    u32 pointCount;

    struct KDNode* parent;
} KDLeaf;

typedef struct {
    u32 value;
    u32 totalTreeSize;
    f32 beta;
    u32 lastUpdate;
} approxCounter;

#endif

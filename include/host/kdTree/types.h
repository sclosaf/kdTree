#ifndef KDTREE_TYPES_H
#define KDTREE_TYPES_H

#include <stdint.h>
#include <stddef.h>

#include "host/environment/constants.h"

typedef enum NodeType
{
    INTERNAL,
    LEAF
} NodeType;

typedef struct point
{
    float* coords;
} __attribute__((aligned(8))) point;

typedef struct KDNode
{
    NodeType type;
    struct KDNode* parent;

    union
    {
        struct
        {
            float splitValue;
            struct KDNode* left;
            struct KDNode* right;
            uint32_t approximateCounter;
            uint8_t splitDim;
        } internal;
        struct
        {
            point* points;
            size_t pointsCount;
        } leaf;
    } data;
} __attribute__((aligned(8))) KDNode;

typedef struct KDNodeReplica
{
    NodeType type;

    KDNode* masterNode;

    struct KDNodeReplica* parent;

    struct KDNodeReplica** descendants;
    size_t descendantCount;
    struct KDNodeReplica** ancestors;
    size_t ancestorCount;

    union
    {
        struct
        {
            struct KDNodeReplica* left;
            struct KDNodeReplica* right;
        } internal;
        struct
        {
            point* points;
            size_t pointsCount;
        } leaf;
    } data;
} KDNodeReplica;

typedef struct KDGroup
{
    KDNode** masterRoots;
    size_t masterRootCount;

    float minSize;
    float maxSize;

    KDNodeReplica** replicas;
    size_t replicaCount;
} KDGroup;

typedef struct KDTree
{
    KDNode* root;
    uint32_t totalPoints;
    uint16_t totalNodes;
    KDGroup** groups;
} KDTree;

#endif

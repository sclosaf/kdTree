#ifndef KDTREE_PRINT_H
#define KDTREE_PRINT_H

#include <stdint.h>
#include <stdbool.h>

#include "kdTree/types.h"

typedef enum Style
{
    COMPACT,
    TREE,
    DETAILED,
    VALIDATE,
    APPROXIMATE,
    STATS,
    MEMORY,
    ONDPU,
    FULL,
} Style;

typedef struct PrintOptions
{
    Style style;
    uint32_t dpuId;
} PrintOptions;

typedef struct NodeStatistics
{
    size_t internal;
    size_t leaf;
    size_t totalPoints;
    int minDepth;
    int maxDepth;
    uint32_t totalCounter;
    size_t maxLeafSize;
    double avgLeafSize;
} NodeStatistics;

typedef struct CounterStatistics
{
    size_t checked;
    size_t inconsistent;
    float minRatio;
    float maxRatio;
    size_t zeroCounters;
} CounterStatistics;

typedef struct Issues
{
    size_t visited;
    size_t invalidParents;
    size_t nullChildren;
    size_t cycles;
} Issues;

void printKDTree(KDNode* root, PrintOptions opt);
void printKDTreeOnDpu(uint32_t dpuId, Style style);
void printKDTreeStats(KDNode* root);

void printNode(KDNode* node, int level, Style style);
void printNodeBrief(KDNode* node);
void printNodeDetailed(KDNode* node);
void printNodeTree(KDNode* node, int level, const char* prefix, bool isLast);
void validateTreeStructure(KDNode* root);
void checkApproximateCounters(KDNode* root);
void printMemoryLayout();

#endif

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <dpu.h>
#include <dpu_types.h>

#include "kdTree/print.h"
#include "kdTree/utils.h"
#include "kdTree/build.h"

#include "environment/constants.h"

static void printSeparator()
{
    printf("\n");

    for(int i = 0; i < 40; ++i)
        printf("=");

    printf("\n");
}

static void printBoldSeparator()
{
    printf("\n");

    for(int i = 0; i < 40; ++i)
        printf("%s=%s", ANSI_BOLD, ANSI_RESET);

    printf("\n");
}

static void computeStats(KDNode* node, NodeStatistics* stats, int depth)
{
    if(!node || !stats)
        return;

    if(depth > stats->maxDepth)
        stats->maxDepth = depth;

    if(depth < stats->minDepth)
        stats->minDepth = depth;

    if(node->type == INTERNAL)
    {
        ++stats->internal;
        stats->totalCounter += node->data.internal.approximateCounter;

        computeStats(node->data.internal.left, stats, depth + 1);
        computeStats(node->data.internal.right, stats, depth + 1);
    }
    else
    {
        ++stats->leaf;
        stats->totalPoints += node->data.leaf.pointsCount;

        if(node->data.leaf.pointsCount > stats->maxLeafSize)
            stats->maxLeafSize = node->data.leaf.pointsCount;
    }
}

static void checkNode(KDNode* node, CounterStatistics* stats)
{
    if(!node || node->type == LEAF || !stats)
        return;

    ++stats->checked;

    if(node->data.internal.approximateCounter == 0)
        ++stats->zeroCounters;

    if(node->data.internal.left && node->data.internal.right)
    {
        uint32_t leftSize = getNodeSize(node->data.internal.left);
        uint32_t rightSize = getNodeSize(node->data.internal.right);
        uint32_t sum = leftSize + rightSize;

        if(sum > 0)
        {
            float ratio = (float)node->data.internal.approximateCounter / sum;

            if(ratio < stats->minRatio)
                stats->minRatio = ratio;

            if(ratio > stats->maxRatio)
                stats->maxRatio = ratio;

            if(ratio < 0.5f || ratio > 2.0f)
            {
                ++stats->inconsistent;

                printf("%sInconsistent counter at node %p:%s cnt=%u vs children (%u+%u=%u) ratio=%.2f\n", ANSI_BOLD, (void*)node, ANSI_RESET, node->data.internal.approximateCounter, leftSize, rightSize, sum, ratio);
            }
        }
    }

    checkNode(node->data.internal.left, stats);
    checkNode(node->data.internal.right, stats);
}

static void validateNode(KDNode* node, KDNode* parent, Issues* issues)
{
    if(!node || !issues)
        return;

    ++issues->visited;

    if(node->parent != parent)
    {
        ++issues->invalidParents;
        printf("%sNode %p has invalid parent%s (expected %p, got %p)\n", ANSI_BOLD, (void*)node, ANSI_RESET, (void*)parent, (void*)node->parent);
    }

    if(node->type == INTERNAL)
    {
        if(!node->data.internal.left && !node->data.internal.right)
        {
            ++issues->nullChildren;
            printf("%sInternal node %p has no children%s\n", ANSI_BOLD, (void*)node, ANSI_RESET);
        }

        if(node->data.internal.left == node || node->data.internal.right == node)
        {
            ++issues->cycles;
            printf("%sCycle detected in node %p%s\n", ANSI_BOLD, (void*)node, ANSI_RESET);
        }

        if(node->data.internal.left)
            validateNode(node->data.internal.left, node, issues);

        if(node->data.internal.right)
            validateNode(node->data.internal.right, node, issues);
    }
}

void printNodeBrief(KDNode* node)
{
    if(!node)
    {
        printf("NULL");
        return;
    }

    if(node->type == INTERNAL)
        printf("I(d%d v%.2f cnt:%s%u%s)", node->data.internal.splitDim, node->data.internal.splitValue, ANSI_BOLD, node->data.internal.approximateCounter, ANSI_RESET);
    else
        printf("L(%s%zu%s pts)", ANSI_BOLD, node->data.leaf.pointsCount, ANSI_RESET);
}

void printNodeDetailed(KDNode* node)
{
    if(!node)
    {
        printf("Node: NULL\n");
        return;
    }

    printf("%sNode Address:%s %p\n", ANSI_BOLD, ANSI_RESET, (void*)node);
    printf("%sType:%s %s\n", ANSI_BOLD, ANSI_RESET, node->type == INTERNAL ? "INTERNAL" : "LEAF");
    printf("%sParent:%s %p\n", ANSI_BOLD, ANSI_RESET, (void*)node->parent);

    if(node->type == INTERNAL)
    {
        printf("%sSplit Dimension:%s %d\n", ANSI_BOLD, ANSI_RESET, node->data.internal.splitDim);
        printf("%sSplit Value:%s %.4f\n", ANSI_BOLD, ANSI_RESET, node->data.internal.splitValue);
        printf("%sApprox Counter:%s %u\n", ANSI_BOLD, ANSI_RESET, node->data.internal.approximateCounter);
        printf("%sLeft Child:%s %p\n", ANSI_BOLD, ANSI_RESET, (void*)node->data.internal.left);
        printf("%sRight Child:%s %p\n", ANSI_BOLD, ANSI_RESET, (void*)node->data.internal.right);

        uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
        uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

        printf("%sChildren Sizes:%s L=%u R=%u (sum=%u)\n", ANSI_BOLD, ANSI_RESET, leftSize, rightSize, leftSize + rightSize);

        if(leftSize + rightSize > 0)
        {
            float ratio = (float)node->data.internal.approximateCounter / (leftSize + rightSize);
            printf("%sCounter Ratio:%s %.2f %s\n", ANSI_BOLD, ANSI_RESET, ratio, (ratio >= 0.5f && ratio <= 2.0f) ? "" : "(INCONSISTENT)");
        }
    }
    else
    {
        printf("%sPoints Count:%s %zu\n", ANSI_BOLD, ANSI_RESET, node->data.leaf.pointsCount);

        if(node->data.leaf.points && node->data.leaf.pointsCount > 0)
        {
            printf("%sFirst 5 points:%s\n", ANSI_BOLD, ANSI_RESET);

            for(size_t i = 0; i < 5 && i < node->data.leaf.pointsCount; ++i)
            {
                printf("  Point %zu: (", i);
                for(int d = 0; d < getConfig()->dimensions; ++d)
                {
                    if(d > 0)
                        printf(", ");

                    printf("%.4f", node->data.leaf.points[i].coords[d]);
                }

                printf(")\n");
            }
        }
    }
}

void printNodeTree(KDNode* node, int level, const char* prefix, bool isLast)
{
    if(!node)
    {
        printf("%s%s── NULL\n", prefix, isLast ? "└" : "├");
        return;
    }

    printf("%s%s── ", prefix, isLast ? "└" : "├");

    if(node->type == INTERNAL)
    {
        printf("[I] d:%d v:%.2f cnt:%s%-4u%s", node->data.internal.splitDim, node->data.internal.splitValue, ANSI_BOLD, node->data.internal.approximateCounter, ANSI_RESET);

        if(node->data.internal.left || node->data.internal.right)
        {
            uint32_t left = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
            uint32_t right = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
            printf(" (L:%u R:%u)", left, right);
        }
    }
    else
    {
        printf("[L] pts:%s%-3zu%s", ANSI_BOLD, node->data.leaf.pointsCount, ANSI_RESET);

        if(node->data.leaf.pointsCount > 0 && node->data.leaf.points)
        {
            printf(" [");

            for(int i = 0; i < 5 && i < node->data.leaf.pointsCount; ++i)
            {
                if(i > 0)
                    printf(", ");

                printf("(");

                for(int d = 0; d < getConfig()->dimensions; ++d)
                {
                    if(d > 0)
                        printf(" ");

                    printf("%.2f", node->data.leaf.points[i].coords[d]);
                }

                printf(")");
            }

            if(node->data.leaf.pointsCount > 5)
                printf(", ...");

            printf("]");
        }
    }

    printf("\n");

    if(node->type == INTERNAL)
    {
        char newPrefix[256];
        snprintf(newPrefix, sizeof(newPrefix), "%s%s   ", prefix, isLast ? " " : "│");

        if(node->data.internal.left)
            printNodeTree(node->data.internal.left, level + 1, newPrefix, !node->data.internal.right);
        if(node->data.internal.right)
            printNodeTree(node->data.internal.right, level + 1, newPrefix, 1);
    }
}

void printNode(KDNode* node, int level, Style style)
{
    switch(style)
    {
        case COMPACT:
            printNodeBrief(node);
            break;
        case TREE:
            printNodeTree(node, level, "", 1);
            break;
        case DETAILED:
            printNodeDetailed(node);
            break;
    }
}

void printKDTree(KDNode* root, Style style)
{
    if(!root)
    {
        printf("KD-Tree: NULL\n");
        return;
    }

    printBoldSeparator();
    printf("%sKD-TREE (Style: ", ANSI_BOLD);

    switch(style)
    {
        case COMPACT:
            printf("COMPACT");
            break;
        case TREE:
            printf("TREE");
            break;
        case DETAILED:
            printf("DETAILED");
            break;
    }

    printf(")%s\n", ANSI_RESET);
    printSeparator();

    switch(style)
    {
        case COMPACT:
            printf("Root: ");
            printNodeBrief(root);
            printf("\n");
            break;
        case TREE:
            printf("%sRoot%s\n", ANSI_BOLD, ANSI_RESET);
            printNodeTree(root, 0, "", 1);
            break;
        case DETAILED:
            printf("\n%s=== ROOT NODE ===%s\n", ANSI_BOLD, ANSI_RESET);
            printNodeDetailed(root);
            printf("\n%s=== TREE TRAVERSAL ===%s\n", ANSI_BOLD, ANSI_RESET);
            printNodeTree(root, 0, "", 1);
            break;
    }

    printKDTreeStats(root);
    printBoldSeparator();
}

void printKDTreeOnDpu(uint32_t dpuId, Style style)
{
    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;

    if(dpuId >= nPim)
        return;

    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    struct dpu_set_t dpu;
    uint32_t currentId = 0;
    bool found = false;

    DPU_FOREACH(set, dpu)
    {
        if(currentId == dpuId)
        {
            found = true;
            break;
        }
        currentId++;
    }

    if(!found)
    {
        dpu_free(set);
        return;
    }

    size_t treeSize = 0;

    DPU_ASSERT(dpu_prepare_xfer(dpu, &treeSize));
    DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, sizeof(size_t), DPU_XFER_DEFAULT));

    if(treeSize == 0)
    {
        dpu_free(set);
        return;
    }

    void* treeData = malloc(treeSize);
    if(!treeData)
    {
        dpu_free(set);
        return;
    }

    DPU_ASSERT(dpu_prepare_xfer(dpu, treeData));
    DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, treeSize, DPU_XFER_DEFAULT));

    KDNode* root = deserializeTree(treeData, treeSize);
    if(root)
    {
        printKDTree(root, style);
        freeKDTree(root);
    }
    else
        printf("Failed to deserialize tree from DPU %u\n", dpuId);

    free(treeData);
    dpu_free(set);
}

void printKDTreeStats(KDNode* root)
{
    if(!root)
        return;

    NodeStatistics stats =
    {
        .internal = 0,
        .leaf = 0,
        .totalPoints = 0,
        .minDepth = 1000,
        .maxDepth = 0,
        .totalCounter = 0,
        .maxLeafSize = 0,
        .avgLeafSize = 0.0
    };

    computeStats(root, &stats, 0);
    stats.avgLeafSize = stats.leaf > 0 ? (double)stats.totalPoints / stats.leaf : 0.0;

    printf("\n%sTREE STATISTICS%s\n", ANSI_BOLD, ANSI_RESET);
    printf("  ├─ Total nodes: %s%zu%s\n", ANSI_BOLD, stats.internal + stats.leaf, ANSI_RESET);
    printf("  ├─ Internal nodes: %zu\n", stats.internal);
    printf("  ├─ Leaf nodes: %zu\n", stats.leaf);
    printf("  ├─ Total points: %s%zu%s\n", ANSI_BOLD, stats.totalPoints, ANSI_RESET);
    printf("  ├─ Depth range: %s%d - %d%s\n", ANSI_BOLD, stats.minDepth, stats.maxDepth, ANSI_RESET);
    printf("  ├─ Leaf size: avg=%.1f max=%s%zu%s\n", stats.avgLeafSize, ANSI_BOLD, stats.maxLeafSize, ANSI_RESET);

    if(stats.internal > 0)
    {
        printf("  ├─ Avg counter: %.2f\n", (double)stats.totalCounter / stats.internal);

        uint32_t leftSize = root->data.internal.left ? getNodeSize(root->data.internal.left) : 0;
        uint32_t rightSize = root->data.internal.right ? getNodeSize(root->data.internal.right) : 0;

        if(leftSize > 0 && rightSize > 0)
        {
            float balance = (float)(leftSize > rightSize ? rightSize : leftSize) / (leftSize + rightSize);
            printf("  └─ Root balance: %s%.2f%s\n", ANSI_BOLD, balance, ANSI_RESET);
        }
    }
}

void printGroupInfo(KDGroup** groups, uint8_t numGroups)
{
    if(!groups) return;

    printBoldSeparator();
    printf("%sGROUP DECOMPOSITION (%u groups)%s\n", ANSI_BOLD, numGroups, ANSI_RESET);
    printSeparator();

    for(uint8_t i = 0; i < numGroups; ++i)
    {
        if(!groups[i])
            continue;

        printf("\n%sGroup %d%s [size: %.0f-%.0f]\n", ANSI_BOLD, i, ANSI_RESET, groups[i]->minSize, groups[i]->maxSize);
        printf("  Nodes: %s%zu%s\n", ANSI_BOLD, groups[i]->count, ANSI_RESET);

        if(groups[i]->count > 0 && groups[i]->rootNodes)
        {
            printf("  Sample roots:\n");
            size_t sample = groups[i]->count < 5 ? groups[i]->count : 5;

            for(size_t j = 0; j < sample; ++j)
            {
                if(groups[i]->rootNodes[j])
                {
                    printf("    %2zu: ", j);
                    printNodeBrief(groups[i]->rootNodes[j]);
                    printf("\n");
                }
            }
        }
    }

    printf("\n");
}

void validateTreeStructure(KDNode* root)
{
    if(!root)
    {
        printf("Tree is NULL\n");
        return;
    }

    Issues issues = {0};

    printf("\n%sVALIDATING TREE STRUCTURE%s\n", ANSI_BOLD, ANSI_RESET);
    validateNode(root, NULL, &issues);

    printf("  ├─ Nodes visited: %s%zu%s\n", ANSI_BOLD, issues.visited, ANSI_RESET);
    printf("  ├─ Invalid parents: %s%zu%s\n", ANSI_BOLD, issues.invalidParents, ANSI_RESET);
    printf("  ├─ Null children: %s%zu%s\n", ANSI_BOLD, issues.nullChildren, ANSI_RESET);
    printf("  └─ Cycles detected: %s%zu%s\n", ANSI_BOLD, issues.cycles, ANSI_RESET);

    if(issues.invalidParents + issues.nullChildren + issues.cycles == 0)
        printf("%sTree structure is valid%s\n", ANSI_BOLD, ANSI_RESET);
    else
        printf("%sTree has %zu issues%s\n", ANSI_BOLD, issues.invalidParents + issues.nullChildren + issues.cycles, ANSI_RESET);
}

void checkApproximateCounters(KDNode* root)
{
    if(!root)
        return;

    CounterStatistics stats = {
        .checked = 0,
        .inconsistent = 0,
        .minRatio = 1000.0f,
        .maxRatio = 0.0f,
        .zeroCounters = 0
    };

    printf("\n%sCHECKING APPROXIMATE COUNTERS%s\n", ANSI_BOLD, ANSI_RESET);
    checkNode(root, &stats);

    printf("  ├─ Nodes checked: %s%zu%s\n", ANSI_BOLD, stats.checked, ANSI_RESET);
    printf("  ├─ Inconsistent: %s%zu%s\n", ANSI_BOLD, stats.inconsistent, ANSI_RESET);
    printf("  ├─ Zero counters: %s%zu%s\n", ANSI_BOLD, stats.zeroCounters, ANSI_RESET);
    printf("  ├─ Ratio range: %s%.2f - %.2f%s\n", ANSI_BOLD, stats.minRatio, stats.maxRatio, ANSI_RESET);

    if(stats.inconsistent == 0)
        printf("  └─ %sAll counters are consistent%s\n", ANSI_BOLD, ANSI_RESET);
    else
        printf("  └─ %sFound %zu inconsistent counters%s\n", ANSI_BOLD, stats.inconsistent, ANSI_RESET);
}

void printMemoryLayout()
{
    struct dpu_set_t set;
    uint32_t nPim = getConfig()->nPim;
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    size_t* nodesPerDpu = calloc(nPim, sizeof(size_t));
    size_t* replicasPerDpu = calloc(nPim, sizeof(size_t));
    size_t* memoryPerDpu = calloc(nPim, sizeof(size_t));

    if(!nodesPerDpu || !replicasPerDpu || !memoryPerDpu)
    {
        free(nodesPerDpu);
        free(replicasPerDpu);
        free(memoryPerDpu);
        dpu_free(set);
        return;
    }

    uint32_t currentId = 0;
    struct dpu_set_t dpu;

    DPU_FOREACH(set, dpu)
    {
        size_t replicaCount = 0;

        DPU_ASSERT(dpu_prepare_xfer(dpu, &replicaCount));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, sizeof(size_t), DPU_XFER_DEFAULT));

        replicasPerDpu[currentId] = replicaCount;

        if(replicaCount > 0)
        {
            ReplicaInfo* infos = malloc(replicaCount * sizeof(ReplicaInfo));
            if(infos)
            {
                DPU_ASSERT(dpu_prepare_xfer(dpu, infos));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, replicaCount * sizeof(ReplicaInfo), DPU_XFER_DEFAULT));

                for(size_t j = 0; j < replicaCount; ++j)
                {
                    nodesPerDpu[currentId] += infos[j].nodeCount;
                    memoryPerDpu[currentId] += sizeof(KDNode) * infos[j].nodeCount;
                }

                free(infos);
            }
        }

        currentId++;
    }

    printBoldSeparator();
    printf("%sMEMORY LAYOUT (%u DPUs)%s\n", ANSI_BOLD, nPim, ANSI_RESET);
    printSeparator();

    size_t totalNodes = 0, totalReplicas = 0, totalMemory = 0;

    for(uint32_t i = 0; i < nPim; ++i)
    {
        printf("%sDPU %2u:%s\n", ANSI_BOLD, i, ANSI_RESET);
        printf("  ├─ Nodes: %s%zu%s\n", ANSI_BOLD, nodesPerDpu[i], ANSI_RESET);
        printf("  ├─ Replicas: %zu\n", replicasPerDpu[i]);
        printf("  ├─ Memory: %.2f KB\n", (double)memoryPerDpu[i] / 1024.0);

        if(i < nPim - 1 && nodesPerDpu[i] > 0)
        {
            double load = (double)nodesPerDpu[i] * nPim / (totalNodes > 0 ? totalNodes : 1);
            printf("  └─ Load factor: %.2f\n", load);
        }

        totalNodes += nodesPerDpu[i];
        totalReplicas += replicasPerDpu[i];
        totalMemory += memoryPerDpu[i];
    }

    printBoldSeparator();

    printf("%sTOTALS:%s\n", ANSI_BOLD, ANSI_RESET);
    printf("  ├─ Nodes: %s%zu%s\n", ANSI_BOLD, totalNodes, ANSI_RESET);
    printf("  ├─ Replicas: %zu\n", totalReplicas);
    printf("  ├─ Memory: %.2f KB\n", (double)totalMemory / 1024.0);
    printf("  └─ Avg nodes/DPU: %s%.1f%s\n", ANSI_BOLD, nPim > 0 ? (double)totalNodes / nPim : 0.0, ANSI_RESET);
    printBoldSeparator();

    free(nodesPerDpu);
    free(replicasPerDpu);
    free(memoryPerDpu);
    dpu_free(set);
}

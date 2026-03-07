#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "kdTree/print.h"
#include "kdTree/serialize.h"
#include "kdTree/deserialize.h"
#include "kdTree/distribute.h"
#include "kdTree/utils.h"
#include "kdTree/counters.h"
#include "kdTree/free.h"

#include "management/dpuManagement.h"

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
        printf("%s=%s", BOLD, RESET);

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

                printf("%sInconsistent counter at node %p:%s cnt=%u vs children (%u+%u=%u) ratio=%.2f\n", BOLD, (void*)node, RESET, node->data.internal.approximateCounter, leftSize, rightSize, sum, ratio);
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
        printf("%sNode %p has invalid parent%s (expected %p, got %p)\n", BOLD, (void*)node, RESET, (void*)parent, (void*)node->parent);
    }

    if(node->type == INTERNAL)
    {
        if(!node->data.internal.left && !node->data.internal.right)
        {
            ++issues->nullChildren;
            printf("%sInternal node %p has no children%s\n", BOLD, (void*)node, RESET);
        }

        if(node->data.internal.left == node || node->data.internal.right == node)
        {
            ++issues->cycles;
            printf("%sCycle detected in node %p%s\n", BOLD, (void*)node, RESET);
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
        printf("I(d%d v%.2f cnt:%s%u%s)", node->data.internal.splitDim, node->data.internal.splitValue, BOLD, node->data.internal.approximateCounter, RESET);
    else
        printf("L(%s%zu%s pts)", BOLD, node->data.leaf.pointsCount, RESET);
}

void printNodeDetailed(KDNode* node)
{
    if(!node)
    {
        printf("Node: NULL\n");
        return;
    }

    printf("%sNode Address:%s %p\n", BOLD, RESET, (void*)node);
    printf("%sType:%s %s\n", BOLD, RESET, node->type == INTERNAL ? "INTERNAL" : "LEAF");
    printf("%sParent:%s %p\n", BOLD, RESET, (void*)node->parent);

    if(node->type == INTERNAL)
    {
        printf("%sSplit Dimension:%s %d\n", BOLD, RESET, node->data.internal.splitDim);
        printf("%sSplit Value:%s %.4f\n", BOLD, RESET, node->data.internal.splitValue);
        printf("%sApprox Counter:%s %u\n", BOLD, RESET, node->data.internal.approximateCounter);
        printf("%sLeft Child:%s %p\n", BOLD, RESET, (void*)node->data.internal.left);
        printf("%sRight Child:%s %p\n", BOLD, RESET, (void*)node->data.internal.right);

        uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
        uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

        printf("%sChildren Sizes:%s L=%u R=%u (sum=%u)\n", BOLD, RESET, leftSize, rightSize, leftSize + rightSize);

        if(leftSize + rightSize > 0)
        {
            float ratio = (float)node->data.internal.approximateCounter / (leftSize + rightSize);
            printf("%sCounter Ratio:%s %.2f %s\n", BOLD, RESET, ratio, (ratio >= 0.5f && ratio <= 2.0f) ? "" : "(INCONSISTENT)");
        }
    }
    else
    {
        printf("%sPoints Count:%s %zu\n", BOLD, RESET, node->data.leaf.pointsCount);

        if(node->data.leaf.points && node->data.leaf.pointsCount > 0)
        {
            printf("%sFirst 5 points:%s\n", BOLD, RESET);

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
        printf("[I] d:%d v:%.2f cnt:%s%-4u%s", node->data.internal.splitDim, node->data.internal.splitValue, BOLD, node->data.internal.approximateCounter, RESET);

        if(node->data.internal.left || node->data.internal.right)
        {
            uint32_t left = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
            uint32_t right = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
            printf(" (L:%u R:%u)", left, right);
        }
    }
    else
    {
        printf("[L] pts:%s%-3zu%s", BOLD, node->data.leaf.pointsCount, RESET);

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
    printf("%sKD-TREE (Style: ", BOLD);

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

    printf(")%s\n", RESET);
    printSeparator();

    switch(style)
    {
        case COMPACT:
            printf("Root: ");
            printNodeBrief(root);
            printf("\n");
            break;
        case TREE:
            printf("%sRoot%s\n", BOLD, RESET);
            printNodeTree(root, 0, "", 1);
            break;
        case DETAILED:
            printf("\n%s=== ROOT NODE ===%s\n", BOLD, RESET);
            printNodeDetailed(root);
            printf("\n%s=== TREE TRAVERSAL ===%s\n", BOLD, RESET);
            printNodeTree(root, 0, "", 1);
            break;
    }

    printKDTreeStats(root);
    printBoldSeparator();
}

void printKDTreeOnDpu(DPUContext* dpuCtx, uint32_t dpuId, Style style)
{
    if(!dpuCtx || dpuId >= dpuCtx->nDpus)
    {
        printf("Invalid DPU ID: %u\n", dpuId);
        return;
    }

    printf("\n%s[Loading tree from DPU %u...]%s\n", BOLD, dpuId, RESET);

    size_t treeSize = 0;
    int ret = dpuTransferDataFromDpu(dpuCtx, dpuId, &treeSize, sizeof(size_t), DPU_XFER_DEFAULT);

    if(ret != 0 || treeSize == 0)
    {
        printf("DPU %u: tree is empty or not accessible\n", dpuId);
        return;
    }

    void* treeData = malloc(treeSize);
    if(!treeData)
    {
        printf("Memory allocation failed\n");
        return;
    }

    ret = dpuTransferDataFromDpu(dpuCtx, dpuId, treeData, treeSize, DPU_XFER_DEFAULT);

    if(ret == 0)
    {
        KDNode* root = deserializeTree(treeData, treeSize);
        if(root)
        {
            printKDTree(root, style);
            freeKDTree(root);
        }
        else
            printf("Failed to deserialize tree from DPU %u\n", dpuId);
    }
    else
        printf("Failed to read tree data from DPU %u\n", dpuId);

    free(treeData);
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

    printf("\n%sTREE STATISTICS%s\n", BOLD, RESET);
    printf("  ├─ Total nodes: %s%zu%s\n", BOLD, stats.internal + stats.leaf, RESET);
    printf("  ├─ Internal nodes: %zu\n", stats.internal);
    printf("  ├─ Leaf nodes: %zu\n", stats.leaf);
    printf("  ├─ Total points: %s%zu%s\n", BOLD, stats.totalPoints, RESET);
    printf("  ├─ Depth range: %s%d - %d%s\n", BOLD, stats.minDepth, stats.maxDepth, RESET);
    printf("  ├─ Leaf size: avg=%.1f max=%s%zu%s\n", stats.avgLeafSize, BOLD, stats.maxLeafSize, RESET);

    if(stats.internal > 0)
    {
        printf("  ├─ Avg counter: %.2f\n", (double)stats.totalCounter / stats.internal);

        uint32_t leftSize = root->data.internal.left ? getNodeSize(root->data.internal.left) : 0;
        uint32_t rightSize = root->data.internal.right ? getNodeSize(root->data.internal.right) : 0;

        if(leftSize > 0 && rightSize > 0)
        {
            float balance = (float)(leftSize > rightSize ? rightSize : leftSize) / (leftSize + rightSize);
            printf("  └─ Root balance: %s%.2f%s\n", BOLD, balance, RESET);
        }
    }
}

void printGroupInfo(KDGroup** groups, uint8_t numGroups)
{
    if(!groups) return;

    printBoldSeparator();
    printf("%sGROUP DECOMPOSITION (%u groups)%s\n", BOLD, numGroups, RESET);
    printSeparator();

    for(uint8_t i = 0; i < numGroups; ++i)
    {
        if(!groups[i])
            continue;

        printf("\n%sGroup %d%s [size: %.0f-%.0f]\n", BOLD, i, RESET, groups[i]->minSize, groups[i]->maxSize);
        printf("  Nodes: %s%zu%s\n", BOLD, groups[i]->count, RESET);

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

    printf("\n%sVALIDATING TREE STRUCTURE%s\n", BOLD, RESET);
    validateNode(root, NULL, &issues);

    printf("  ├─ Nodes visited: %s%zu%s\n", BOLD, issues.visited, RESET);
    printf("  ├─ Invalid parents: %s%zu%s\n", BOLD, issues.invalidParents, RESET);
    printf("  ├─ Null children: %s%zu%s\n", BOLD, issues.nullChildren, RESET);
    printf("  └─ Cycles detected: %s%zu%s\n", BOLD, issues.cycles, RESET);

    if(issues.invalidParents + issues.nullChildren + issues.cycles == 0)
        printf("%sTree structure is valid%s\n", BOLD, RESET);
    else
        printf("%sTree has %zu issues%s\n", BOLD,
               issues.invalidParents + issues.nullChildren + issues.cycles, RESET);
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

    printf("\n%sCHECKING APPROXIMATE COUNTERS%s\n", BOLD, RESET);
    checkNode(root, &stats);

    printf("  ├─ Nodes checked: %s%zu%s\n", BOLD, stats.checked, RESET);
    printf("  ├─ Inconsistent: %s%zu%s\n", BOLD, stats.inconsistent, RESET);
    printf("  ├─ Zero counters: %s%zu%s\n", BOLD, stats.zeroCounters, RESET);
    printf("  ├─ Ratio range: %s%.2f - %.2f%s\n", BOLD, stats.minRatio, stats.maxRatio, RESET);

    if(stats.inconsistent == 0)
        printf("  └─ %sAll counters are consistent%s\n", BOLD, RESET);
    else
        printf("  └─ %sFound %zu inconsistent counters%s\n", BOLD, stats.inconsistent, RESET);
}

void printMemoryLayout(DPUContext* dpuCtx)
{
    if(!dpuCtx)
        return;

    size_t P = dpuCtx->nDpus;
    size_t* nodesPerDpu = calloc(P, sizeof(size_t));
    size_t* replicasPerDpu = calloc(P, sizeof(size_t));
    size_t* memoryPerDpu = calloc(P, sizeof(size_t));

    if(!nodesPerDpu || !replicasPerDpu || !memoryPerDpu)
    {
        free(nodesPerDpu);
        free(replicasPerDpu);
        free(memoryPerDpu);
        return;
    }

    for(size_t i = 0; i < P; ++i)
    {
        size_t replicaCount = 0;
        int ret = dpuTransferDataFromDpu(dpuCtx, (uint32_t)i, &replicaCount, sizeof(size_t), DPU_XFER_DEFAULT);

        if(ret == 0)
        {
            replicasPerDpu[i] = replicaCount;

            if(replicaCount > 0)
            {
                ReplicaInfo* infos = malloc(replicaCount * sizeof(ReplicaInfo));
                if(infos)
                {
                    ret = dpuTransferDataFromDpu(dpuCtx, (uint32_t)i, infos, replicaCount * sizeof(ReplicaInfo), DPU_XFER_DEFAULT);

                    if(ret == 0)
                        for(size_t j = 0; j < replicaCount; ++j)
                        {
                            nodesPerDpu[i] += infos[j].nodeCount;
                            memoryPerDpu[i] += sizeof(KDNode) * infos[j].nodeCount;
                        }

                    free(infos);
                }
            }
        }
    }

    printBoldSeparator();
    printf("%sMEMORY LAYOUT (%zu DPUs)%s\n", BOLD, P, RESET);
    printSeparator();

    size_t totalNodes = 0, totalReplicas = 0, totalMemory = 0;

    for(size_t i = 0; i < P; ++i)
    {
        printf("%sDPU %2zu:%s\n", BOLD, i, RESET);
        printf("  ├─ Nodes: %s%zu%s\n", BOLD, nodesPerDpu[i], RESET);
        printf("  ├─ Replicas: %zu\n", replicasPerDpu[i]);
        printf("  ├─ Memory: %.2f KB\n", (double)memoryPerDpu[i] / 1024.0);

        if(i < P - 1 && nodesPerDpu[i] > 0)
        {
            double load = (double)nodesPerDpu[i] * P / (totalNodes > 0 ? totalNodes : 1);
            printf("  └─ Load factor: %.2f\n", load);
        }

        totalNodes += nodesPerDpu[i];
        totalReplicas += replicasPerDpu[i];
        totalMemory += memoryPerDpu[i];
    }

    printBoldSeparator();

    printf("%sTOTALS:%s\n", BOLD, RESET);
    printf("  ├─ Nodes: %s%zu%s\n", BOLD, totalNodes, RESET);
    printf("  ├─ Replicas: %zu\n", totalReplicas);
    printf("  ├─ Memory: %.2f KB\n", (double)totalMemory / 1024.0);
    printf("  └─ Avg nodes/DPU: %s%.1f%s\n", BOLD, P > 0 ? (double)totalNodes / P : 0.0, RESET);
    printBoldSeparator();

    free(nodesPerDpu);
    free(replicasPerDpu);
    free(memoryPerDpu);
}

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "kdTree/print.h"
#include "kdTree/serialize.h"
#include "kdTree/deserialize.h"
#include "kdTree/utils.h"
#include "kdTree/counters.h"

#include "management/dpuManagement.h"

static void printIndent(int level)
{
    for(int i = 0; i < level; ++i)
        printf("  ");
}

static void printHorizontalLine(int width)
{
    printf("\n");

    for(int i = 0; i < width; ++i)
        printf("─");

    printf("\n");
}

static void printSeparator()
{
    printf("\n");

    for(int i = 0; i < 50; ++i)
        printf("=");

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
        stats->internal++;
        stats->totalCounter += node->data.internal.approximateCounter;
        computeStats(node->data.internal.left, stats, depth + 1);
        computeStats(node->data.internal.right, stats, depth + 1);
    }
    else
    {
        stats->leaf++;
        stats->totalPoints += node->data.leaf.pointsCount;

        if(node->data.leaf.pointsCount > stats->maxLeafSize)
            stats->maxLeafSize = node->data.leaf.pointsCount;
    }
}

static void checkNode(KDNode* node, CounterStatistics* stats)
{
    if(!node || node->type == LEAF || !stats)
        return;

    stats->checked++;

    if(node->data.internal.approximateCounter == 0)
        stats->zeroCounters++;

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
                stats->inconsistent++;

                printf("Inconsistent counter at node %p: cnt=%u vs children (%u+%u=%u) ratio=%.2f\n", (void*)node, node->data.internal.approximateCounter, leftSize, rightSize, sum, ratio);
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

    issues->visited++;

    if(node->parent != parent)
    {
        issues->invalidParents++;
        printf("Node %p has invalid parent (expected %p, got %p)\n", (void*)node, (void*)parent, (void*)node->parent);
    }

    if(node->type == INTERNAL)
    {
        if(!node->data.internal.left && !node->data.internal.right)
        {
            issues->nullChildren++;
            printf("Internal node %p has no children\n", (void*)node);
        }

        if(node->data.internal.left == node || node->data.internal.right == node)
        {
            issues->cycles++;
            printf("Cycle detected in node %p\n", (void*)node);
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
        printf("I(d%d v%.2f cnt:%u)", node->data.internal.splitDim, node->data.internal.splitValue, node->data.internal.approximateCounter);
    else
        printf("L(%zu pts)", node->data.leaf.pointsCount);
}

void printNodeDetailed(KDNode* node)
{
    if(!node)
    {
        printf("Node: NULL\n");
        return;
    }

    printf("Node Address: %p\n", (void*)node);
    printf("Type: %s\n", node->type == INTERNAL ? "INTERNAL" : "LEAF");
    printf("Parent: %p\n", (void*)node->parent);

    if(node->type == INTERNAL)
    {
        printf("Split Dimension: %d\n", node->data.internal.splitDim);
        printf("Split Value: %.4f\n", node->data.internal.splitValue);
        printf("Approx Counter: %u\n", node->data.internal.approximateCounter);
        printf("Left Child: %p\n", (void*)node->data.internal.left);
        printf("Right Child: %p\n", (void*)node->data.internal.right);

        uint32_t leftSize = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
        uint32_t rightSize = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;

        printf("Children Sizes: L=%u R=%u (sum=%u)\n", leftSize, rightSize, leftSize + rightSize);

        if(leftSize + rightSize > 0)
        {
            float ratio = (float)node->data.internal.approximateCounter / (leftSize + rightSize);
            printf("Counter Ratio: %.2f %s\n", ratio, (ratio >= 0.5f && ratio <= 2.0f) ? "(OK)" : "(INCONSISTENT)");
        }
    }
    else
    {
        printf("Points Count: %zu\n", node->data.leaf.pointsCount);

        if(node->data.leaf.points && node->data.leaf.pointsCount > 0)
        {
            printf("First 5 points:\n");

            for(size_t i = 0; i < 5; ++i)
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
        printf("[I] d:%d v:%.2f cnt:%-4u", node->data.internal.splitDim, node->data.internal.splitValue, node->data.internal.approximateCounter);

        if(node->data.internal.left || node->data.internal.right)
        {
            uint32_t left = node->data.internal.left ? getNodeSize(node->data.internal.left) : 0;
            uint32_t right = node->data.internal.right ? getNodeSize(node->data.internal.right) : 0;
            printf(" (L:%u R:%u)", left, right);
        }
    }
    else
    {
        printf("[L] pts:%-3zu", node->data.leaf.pointsCount);

        if(node->data.leaf.pointsCount > 0 && node->data.leaf.points)
        {
            printf(" [");

            for(int i = 0; i < 5; ++i)
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

            if(node->data.leaf.pointsCount > 2)
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

    printSeparator();
    printf("KD-TREE (Style: ");

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

    printf(")\n");
    printSeparator();

    switch(style)
    {
        case COMPACT:
            printf("Root: ");
            printNodeBrief(root);
            printf("\n");
            break;

        case TREE:
            printf("Root\n");
            printNodeTree(root, 0, "", 1);
            break;

        case DETAILED:
            printf("\n=== ROOT NODE ===\n");
            printNodeDetailed(root);

            printf("\n=== TREE TRAVERSAL ===\n");
            printNodeTree(root, 0, "", 1);
            break;
    }

    printKDTreeStats(root);
    printSeparator();
}

void printKDTreeOnDpu(DPUContext* dpuCtx, uint32_t dpuId, Style style)
{
    if(!dpuCtx || dpuId >= dpuCtx->nDpus)
    {
        printf("Invalid DPU ID: %u\n", dpuId);
        return;
    }

    printf("\n[Loading tree from DPU %u...]\n", dpuId);

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
            char title[256];
            snprintf(title, sizeof(title), "DPU %u Tree", dpuId);
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

    printf("\nTREE STATISTICS\n");
    printf("  ├─ Total nodes: %zu\n", stats.internal + stats.leaf);
    printf("  ├─ Internal nodes: %zu\n", stats.internal);
    printf("  ├─ Leaf nodes: %zu\n", stats.leaf);
    printf("  ├─ Total points: %zu\n", stats.totalPoints);
    printf("  ├─ Depth range: %d - %d\n", stats.minDepth, stats.maxDepth);
    printf("  ├─ Leaf size: avg=%.1f max=%zu\n", stats.avgLeafSize, stats.maxLeafSize);

    if(stats.internal > 0)
    {
        printf("  └─ Avg counter: %.2f\n", (double)stats.totalCounter / stats.internal);

        uint32_t leftSize = root->data.internal.left ? getNodeSize(root->data.internal.left) : 0;
        uint32_t rightSize = root->data.internal.right ? getNodeSize(root->data.internal.right) : 0;

        if(leftSize > 0 && rightSize > 0)
        {
            float balance = (float)(leftSize > rightSize ? rightSize : leftSize) / (leftSize + rightSize);
            printf("  └─ Root balance: %.2f\n", balance);
        }
    }
}

void printGroupInfo(KDGroup** groups, uint8_t numGroups)
{
    if(!groups) return;

    printSeparator();
    printf("GROUP DECOMPOSITION (%u groups)\n", numGroups);
    printSeparator();

    for(uint8_t i = 0; i < numGroups; ++i)
    {
        if(!groups[i])
            continue;

        printf("\nGroup %d [size: %.0f-%.0f]\n", i, groups[i]->minSize, groups[i]->maxSize);
        printf("  Nodes: %zu\n", groups[i]->count);

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

    printf("\nVALIDATING TREE STRUCTURE\n");
    validate(root, NULL, &issues);

    printf("  ├─ Nodes visited: %zu\n", issues.visited);
    printf("  ├─ Invalid parents: %zu\n", issues.invalidParents);
    printf("  ├─ Null children: %zu\n", issues.nullChildren);
    printf("  └─ Cycles detected: %zu\n", issues.cycles);

    if(issues.invalidParents + issues.nullChildren + issues.cycles == 0)
        printf("Tree structure is valid\n");
    else
        printf("Tree has %zu issues\n",
               issues.invalidParents + issues.nullChildren + issues.cycles);
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

    printf("\nCHECKING APPROXIMATE COUNTERS\n");
    checkNode(root, &stats);

    printf("  ├─ Nodes checked: %zu\n", stats.checked);
    printf("  ├─ Inconsistent: %zu\n", stats.inconsistent);
    printf("  ├─ Zero counters: %zu\n", stats.zeroCounters);
    printf("  ├─ Ratio range: %.2f - %.2f\n",
           stats.minRatio, stats.maxRatio);

    if(stats.inconsistent == 0)
        printf("  └─ All counters are consistent\n");
    else
        printf("  └─ Found %zu inconsistent counters\n", stats.inconsistent);
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
                    {
                        for(size_t j = 0; j < replicaCount; ++j)
                        {
                            nodesPerDpu[i] += infos[j].nodeCount;
                            memoryPerDpu[i] += sizeof(KDNode) * infos[j].nodeCount;
                        }
                    }

                    free(infos);
                }
            }
        }
    }

    printSeparator();
    printf("MEMORY LAYOUT (%zu DPUs)\n", P);
    printSeparator();

    size_t totalNodes = 0, totalReplicas = 0, totalMemory = 0;

    for(size_t i = 0; i < P; ++i)
    {
        printf("DPU %2zu:\n", i);
        printf("  ├─ Nodes: %zu\n", nodesPerDpu[i]);
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

    printSeparator();

    printf("TOTALS:\n");
    printf("  ├─ Nodes: %zu\n", totalNodes);
    printf("  ├─ Replicas: %zu\n", totalReplicas);
    printf("  ├─ Memory: %.2f KB\n", (double)totalMemory / 1024.0);
    printf("  └─ Avg nodes/DPU: %.1f\n", P > 0 ? (double)totalNodes / P : 0.0);
    printSeparator();

    free(nodesPerDpu);
    free(replicasPerDpu);
    free(memoryPerDpu);
}

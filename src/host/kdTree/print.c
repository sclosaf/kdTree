#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <dpu.h>
#include <dpu_types.h>

#include "host/kdTree/print.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/build.h"
#include "host/kdTree/free.h"
#include "host/kdTree/serialization.h"

#include "host/environment/constants.h"
#include "host/environment/init.h"

#include "host/management/logging.h"

static void printSeparator()
{
    for(int i = 0; i < 40; ++i)
        printf("=");
}

static void printBoldSeparator()
{
    for(int i = 0; i < 40; ++i)
        printf("%s=%s", ANSI_BOLD, ANSI_RESET);
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
                for(int d = 0; d < getDimensions(); ++d)
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

                for(int d = 0; d < getDimensions(); ++d)
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

void printKDTree(KDNode* root, PrintOptions* opt)
{
    logMessage("Printing KD-tree", DEBUG);

    if(!opt)
    {
        logMessage("Null PrintOptions provided", ERROR);
        return;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Print style: %d", opt->style);
    logMessage(msg, DEBUG);

    printBoldSeparator();
    printf("\n%sKD-TREE (Style: ", ANSI_BOLD);

    switch(opt->style)
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
        case VALIDATE:
            printf("VALIDATE");
            break;
        case APPROXIMATE:
            printf("APPROXIMATE COUNTER CHECK");
            break;
        case REPLICAS:
            printf("REPLICAS");
            break;
        case STATS:
            printf("STATISTICS");
            break;
        case MEMORY:
            printf("MEMORY");
            break;
        case ONDPU:
            printf("ON DPU %u", opt->dpuId);
            break;
        default:
            printf("UNKNOWN");
            break;
    }

    printf(")%s\n", ANSI_RESET);
    printBoldSeparator();

    switch(opt->style)
    {
        case COMPACT:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }

            printf("Root: ");
            printNodeBrief(root);
            printf("\n");
            break;

        case TREE:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }
            printf("%sRoot%s\n", ANSI_BOLD, ANSI_RESET);
            printNodeTree(root, 0, "", 1);
            break;

        case DETAILED:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }
            printf("\n%s=== ROOT NODE ===%s\n", ANSI_BOLD, ANSI_RESET);
            printNodeDetailed(root);
            break;

        case VALIDATE:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }

            validateTreeStructure(root);
            break;

        case APPROXIMATE:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }

            checkApproximateCounters(root);
            break;

        case REPLICAS:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }

            printf("\n%s=== MASTER TREE ===%s\n", ANSI_BOLD, ANSI_RESET);
            printReplicas(getData()->tree->groups);
            break;

        case STATS:
            if(!root)
            {
                printf("KD-Tree: NULL\n");
                break;
            }

            printKDTreeStats(root);
            break;

        case MEMORY:
            printMemoryLayout();
            break;

        case ONDPU:
            if(opt->dpuId >= getNPim())
            {
                printf("Error: Invalid DPU ID %u (max: %u)\n", opt->dpuId, getNPim() - 1);
                break;
            }

            printKDTreeOnDpu(opt->dpuId, opt->style);
            break;

        default:
            logMessage("Unknown print style requested", ERROR);
            printf("UNKNOWN STYLE\n");
            break;
    }
}

void printKDTreeOnDpu(uint32_t dpuId, Style style)
{
    logMessage("Printing KD-tree from DPU", DEBUG);

    struct dpu_set_t set;
    uint32_t nPim = getNPim();

    if(dpuId >= nPim)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Invalid DPU ID %u for print (max: %u)", dpuId, nPim - 1);
        logMessage(msg, ERROR);
        return;
    }

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
        ++currentId;
    }

    if(!found)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "DPU %u not found in set", dpuId);
        logMessage(msg, ERROR);
        dpu_free(set);
        return;
    }

    if(style == MEMORY)
    {
        PrintOptions opt = { .style = MEMORY, .dpuId = dpuId };
        printKDTree(NULL, &opt);
        dpu_free(set);
        return;
    }

    size_t treeSize = 0;

    DPU_ASSERT(dpu_prepare_xfer(dpu, &treeSize));
    DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, sizeof(size_t), DPU_XFER_DEFAULT));

    if(treeSize == 0)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "No tree data found on DPU %u", dpuId);
        logMessage(msg, ERROR);
        printf("No tree found on DPU %u\n", dpuId);
    }
    else
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Retrieving %zu bytes of tree data from DPU %u", treeSize, dpuId);
        logMessage(msg, DEBUG);

        void* treeData = malloc(treeSize);
        if(treeData)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, treeData));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, treeSize, DPU_XFER_DEFAULT));

            KDNode* root = deserializeTree(treeData, treeSize);
            if(root)
            {
                snprintf(msg, sizeof(msg), "Tree deserialized from DPU %u successfully", dpuId);
                logMessage(msg, INFO);

                PrintOptions opt = { .style = style, .dpuId = dpuId };
                printKDTree(root, &opt);
                freeKDTree(root);
            }
            else
            {
                snprintf(msg, sizeof(msg), "Failed to deserialize tree from DPU %u", dpuId);
                logMessage(msg, ERROR);
                printf("Failed to deserialize tree from DPU %u\n", dpuId);
            }

            free(treeData);
        }
        else
        {
            snprintf(msg, sizeof(msg), "Failed to allocate buffer for DPU %u tree data", dpuId);
            logMessage(msg, ERROR);
        }
    }

    printf("\n");
    printNodeLocationMapOnDpu(dpuId);

    dpu_free(set);
}

void printKDTreeStats(KDNode* root)
{
    logMessage("Computing and printing tree statistics", DEBUG);

    if(!root)
    {
        logMessage("Null root provided for stats", ERROR);
        return;
    }

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

    char msg[256];
    snprintf(msg, sizeof(msg), "Stats: %zu internal, %zu leaf, %zu points, depth %d-%d", stats.internal, stats.leaf, stats.totalPoints, stats.minDepth, stats.maxDepth);
    logMessage(msg, INFO);

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

void validateTreeStructure(KDNode* root)
{
    logMessage("Validating tree structure", DEBUG);

    if(!root)
    {
        logMessage("Null root provided for validation", ERROR);
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

    size_t totalIssues = issues.invalidParents + issues.nullChildren + issues.cycles;

    if(totalIssues == 0)
    {
        logMessage("Tree structure valid", INFO);
        printf("%sTree structure is valid%s\n", ANSI_BOLD, ANSI_RESET);
    }
    else
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Tree validation found %zu issues", totalIssues);
        logMessage(msg, ERROR);
        printf("%sTree has %zu issues%s\n", ANSI_BOLD, totalIssues, ANSI_RESET);
    }
}


void checkApproximateCounters(KDNode* root)
{
    logMessage("Checking approximate counters", DEBUG);

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
    {
        logMessage("All approximate counters consistent", INFO);
        printf("  └─ %sAll counters are consistent%s\n", ANSI_BOLD, ANSI_RESET);
    }
    else
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Found %zu inconsistent approximate counters", stats.inconsistent);
        logMessage(msg, ERROR);
        printf("  └─ %sFound %zu inconsistent counters%s\n", ANSI_BOLD, stats.inconsistent, ANSI_RESET);
    }
}

void printMemoryLayout()
{
    logMessage("Printing memory layout", DEBUG);

    struct dpu_set_t set;
    uint32_t nPim = getNPim();
    DPU_ASSERT(dpu_alloc(nPim, NULL, &set));

    size_t* nodesPerDpu = calloc(nPim, sizeof(size_t));
    size_t* replicasPerDpu = calloc(nPim, sizeof(size_t));
    size_t* memoryPerDpu = calloc(nPim, sizeof(size_t));

    if(!nodesPerDpu || !replicasPerDpu || !memoryPerDpu)
    {
        logMessage("Failed to allocate memory layout arrays", ERROR);
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
            else
            {
                char msg[256];
                snprintf(msg, sizeof(msg), "Failed to allocate ReplicaInfo buffer for DPU %u", currentId);
                logMessage(msg, ERROR);
            }
        }

        ++currentId;
    }

    printBoldSeparator();
    printf("%sMEMORY LAYOUT (%u DPUs)%s\n", ANSI_BOLD, nPim, ANSI_RESET);
    printBoldSeparator();

    size_t totalNodes = 0, totalReplicas = 0, totalMemory = 0;

    for(uint32_t i = 0; i < nPim; ++i)
    {
        printf("%sDPU %2u:%s\n", ANSI_BOLD, i, ANSI_RESET);
        printf("  ├─ Nodes: %s%zu%s\n", ANSI_BOLD, nodesPerDpu[i], ANSI_RESET);
        printf("  ├─ Replicas: %zu\n", replicasPerDpu[i]);
        printf("  ├─ Memory: %.2f KB\n", (double)memoryPerDpu[i] / 1024.0);

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

    char msg[256];
    snprintf(msg, sizeof(msg), "Memory layout: %zu total nodes, %zu replicas, %.2f KB across %u DPUs", totalNodes, totalReplicas, (double)totalMemory / 1024.0, nPim);
    logMessage(msg, INFO);

    Data* data = getData();
    if(data && data->map)
    {
        NodeLocationMap* map = data->map;
        printf("\n%sMAPPED NODES:%s\n", ANSI_BOLD, ANSI_RESET);
        printf("  ├─ Total mapped: %s%zu%s\n", ANSI_BOLD, map->count, ANSI_RESET);

        size_t* mappedPerDpu = calloc(nPim + 1, sizeof(size_t));
        for(size_t i = 0; i < map->count; ++i)
        {
            uint32_t idx = (map->dpuIds[i] == UINT32_MAX) ? nPim : map->dpuIds[i];
            ++mappedPerDpu[idx];
        }

        printf("  ├─ Host mapped: %zu\n", mappedPerDpu[nPim]);
        for(uint32_t d = 0; d < nPim && d < 5; ++d)
            if(mappedPerDpu[d] > 0)
                printf("  ├─ DPU %u mapped: %zu\n", d, mappedPerDpu[d]);

        free(mappedPerDpu);
    }

    printBoldSeparator();

    free(nodesPerDpu);
    free(replicasPerDpu);
    free(memoryPerDpu);
    dpu_free(set);
}

void printNodeLocationMap()
{
    logMessage("Printing node location map", DEBUG);

    Data* data = getData();
    if(!data || !data->map)
    {
        logMessage("Node location map unavailable", ERROR);
        return;
    }

    NodeLocationMap* map = data->map;

    printf("Capacity: %s%zu%s\n", ANSI_BOLD, map->capacity, ANSI_RESET);
    printf("Count: %s%zu%s\n", ANSI_BOLD, map->count, ANSI_RESET);

    size_t memoryUsage = map->capacity * (sizeof(KDNode*) + sizeof(uint32_t) + sizeof(uint64_t));
    printf("Memory usage: %.2f KB\n", (double)memoryUsage / 1024.0);

    char msg[256];
    snprintf(msg, sizeof(msg), "Node location map: %zu entries, %.2f KB", map->count, (double)memoryUsage / 1024.0);
    logMessage(msg, INFO);

    if(map->count == 0)
    {
        logMessage("Node location map is empty", DEBUG);
        printf("\n%sMap is empty%s\n", ANSI_BOLD, ANSI_RESET);
        printSeparator();
        return;
    }

    printf("\n%sEntries by location:%s\n", ANSI_BOLD, ANSI_RESET);

    uint32_t nPim = getNPim();
    size_t* countsPerDpu = calloc(nPim + 1, sizeof(size_t));
    size_t* internalPerDpu = calloc(nPim + 1, sizeof(size_t));

    for(size_t i = 0; i < map->count; ++i)
    {
        uint32_t idx = (map->dpuIds[i] == UINT32_MAX) ? nPim : map->dpuIds[i];
        ++countsPerDpu[idx];
        if(map->nodes[i] && map->nodes[i]->type == INTERNAL)
            ++internalPerDpu[idx];
    }

    printf("  %sHOST (CPU):%s %zu nodes (%zu internal)\n", ANSI_BOLD, ANSI_RESET, countsPerDpu[nPim], internalPerDpu[nPim]);

    for(uint32_t d = 0; d < nPim; ++d)
        if(countsPerDpu[d] > 0)
            printf("  %sDPU %u:%s %zu nodes (%zu internal)\n", ANSI_BOLD, d, ANSI_RESET, countsPerDpu[d], internalPerDpu[d]);

    free(countsPerDpu);
    free(internalPerDpu);

    for(size_t i = 0; i < map->count; ++i)
    {
        const char* location = (map->dpuIds[i] == UINT32_MAX) ? "HOST" : "DPU";
        const char* type = (map->nodes[i] && map->nodes[i]->type == INTERNAL) ? "I" : "L";
        uint32_t dpuId = (map->dpuIds[i] == UINT32_MAX) ? 0 : map->dpuIds[i];

        printf("  [%s%3zu%s] %s%s%s %s%p%s -> %s%s %u %s@ 0x%016llx\n", ANSI_BOLD, i, ANSI_RESET, ANSI_BOLD, type, ANSI_RESET, ANSI_BOLD, (void*)map->nodes[i], ANSI_RESET, ANSI_BOLD, location, dpuId, ANSI_RESET, (unsigned long long)map->dpuAddresses[i]);
    }

    printSeparator();
}

void printNodeLocationMapOnDpu(uint32_t dpuId)
{
    logMessage("Printing node location map from DPU", DEBUG);

    struct dpu_set_t set;
    uint32_t nPim = getNPim();

    if(dpuId >= nPim)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "Invalid DPU ID %u for location map print (max: %u)", dpuId, nPim - 1);
        logMessage(msg, ERROR);
        return;
    }

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
        ++currentId;
    }

    if(!found)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "DPU %u not found in set for location map print", dpuId);
        logMessage(msg, ERROR);
        dpu_free(set);
        return;
    }

    size_t mapSize = 0;
    DPU_ASSERT(dpu_prepare_xfer(dpu, &mapSize));
    DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "mapSize", 0, sizeof(size_t), DPU_XFER_DEFAULT));

    char msg[256];
    snprintf(msg, sizeof(msg), "DPU %u location map has %zu entries", dpuId, mapSize);
    logMessage(msg, INFO);

    printf("DPU %u has %s%zu%s registered nodes\n", dpuId, ANSI_BOLD, mapSize, ANSI_RESET);

    if(mapSize > 0)
    {
        uint64_t* dpuAddresses = malloc(mapSize * sizeof(uint64_t));
        if(dpuAddresses)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, dpuAddresses));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "mapData", 0, mapSize * sizeof(uint64_t), DPU_XFER_DEFAULT));

            printf("\n%sRegistered node addresses:%s\n", ANSI_BOLD, ANSI_RESET);

            for(size_t i = 0; i < mapSize; ++i)
                printf("  [%s%3zu%s] 0x%016llx\n", ANSI_BOLD, i, ANSI_RESET, (unsigned long long)dpuAddresses[i]);

            free(dpuAddresses);
        }
        else
        {
            snprintf(msg, sizeof(msg), "Failed to allocate address buffer for DPU %u location map", dpuId);
            logMessage(msg, ERROR);
        }
    }

    dpu_free(set);
}

void printReplicas(KDGroup** groups)
{
    logMessage("Printing replicas", DEBUG);

    if(!groups)
    {
        logMessage("Null groups provided for replica print", ERROR);
        printf("%sNo groups available%s\n", ANSI_BOLD, ANSI_RESET);
        return;
    }

    bool hasReplicas = false;
    for(int i = 1; groups[i] != NULL; ++i)
        if(groups[i]->replicas && groups[i]->replicaCount > 0)
        {
            hasReplicas = true;
            break;
        }

    if(!hasReplicas)
    {
        logMessage("No replicas found in any group", DEBUG);
        printf("\n%sNo replicas found in any group%s\n", ANSI_BOLD, ANSI_RESET);
        return;
    }

    for(int i = 1; groups[i] != NULL; ++i)
    {
        if(groups[i]->replicas && groups[i]->replicaCount > 0)
        {
            char msg[256];
            snprintf(msg, sizeof(msg), "Printing %zu replicas for group %d (size: %.0f-%.0f)", groups[i]->replicaCount, i, groups[i]->minSize, groups[i]->maxSize);

            logMessage(msg, INFO);

            printf("\n%s=== GROUP %d REPLICAS (size: %.0f-%.0f) ===%s\n", ANSI_BOLD, i, groups[i]->minSize, groups[i]->maxSize, ANSI_RESET);
            printf("  Total replicas: %zu\n", groups[i]->replicaCount);

            for(size_t j = 0; j < groups[i]->replicaCount; ++j)
            {
                printf("\n  Replica %zu (master: %p):\n", j, (void*)groups[i]->replicas[j]->masterNode);
                printReplicaTree(groups[i]->replicas[j], 1, "  ", true);
            }
        }
    }
}

void printReplicaTree(KDNodeReplica* replica, int level, const char* prefix, bool isLast)
{
    if(!replica)
    {
        printf("%s%s── NULL (replica)\n", prefix, isLast ? "└" : "├");
        return;
    }

    printf("%s%s── ", prefix, isLast ? "└" : "├");

    if(replica->type == INTERNAL)
    {
        if(replica->data.internal.left && replica->data.internal.left->masterNode)
            printf("[R-I] d:%d v:%.2f (master:%p)", replica->data.internal.left->masterNode->data.internal.splitDim, replica->data.internal.left->masterNode->data.internal.splitValue, (void*)replica->masterNode);
        else
            printf("[R-I] (master:%p)", (void*)replica->masterNode);

        printf(" [desc:%zu anc:%zu]", replica->descendantCount, replica->ancestorCount);
    }
    else
        printf("[R-L] pts:%zu (master:%p)", replica->data.leaf.pointsCount, (void*)replica->masterNode);

    printf("\n");

    if(replica->type == INTERNAL)
    {
        char newPrefix[256];
        snprintf(newPrefix, sizeof(newPrefix), "%s%s   ", prefix, isLast ? " " : "│");

        if(replica->data.internal.left)
            printReplicaTree(replica->data.internal.left, level + 1, newPrefix, !replica->data.internal.right);

        if(replica->data.internal.right)
            printReplicaTree(replica->data.internal.right, level + 1, newPrefix, true);
    }
}

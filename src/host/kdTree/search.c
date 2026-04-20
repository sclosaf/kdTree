#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dpu.h>
#include <dpu_types.h>

#include "host/kdTree/search.h"
#include "host/kdTree/build.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/free.h"

#include "host/environment/constants.h"
#include "host/environment/init.h"

#include "host/loader/dispatcher.h"
#include "host/management/logging.h"

typedef struct NodeGroup
{
    KDNode* node;
    uint32_t* indices;
    uint32_t count;
} NodeGroup;

SearchBatch* initSearchBatch(point** queries, size_t size)
{
    logMessage("Initializing search batch", DEBUG);

    SearchBatch* batch = (SearchBatch*)malloc(sizeof(SearchBatch));
    if(!batch)
    {
        logMessage("Failed to allocate search batch", ERROR);
        return NULL;
    }

    batch->queries = queries;
    batch->size = size;
    batch->localReplicas = NULL;
    batch->results = (KDNode**)calloc(size, sizeof(KDNode*));

    if(!batch->results)
    {
        logMessage("Failed to allocate results array", ERROR);
        free(batch);
        return NULL;
    }

    logMessage("Search batch initialized", INFO);
    return batch;
}

PushPullContext* initPushPullContext()
{
    logMessage("Initializing push-pull context", DEBUG);

    PushPullContext* context = (PushPullContext*)malloc(sizeof(PushPullContext));
    if(!context)
    {
        logMessage("Failed to allocate push-pull context", ERROR);
        return NULL;
    }

    context->nodeVisits = 0;
    context->fanout = 2;
    context->forcePull = false;
    context->numGroups = calculateNumGroups();

    context->groupThresholds = (uint32_t*)malloc(context->numGroups * sizeof(uint32_t));
    context->groupAccessCounts = (uint32_t*)calloc(context->numGroups, sizeof(uint32_t));
    context->groupPullDecision = (bool*)calloc(context->numGroups, sizeof(bool));

    if(!context->groupThresholds || !context->groupAccessCounts || !context->groupPullDecision)
    {
        logMessage("Failed to allocate group arrays", ERROR);
        free(context->groupThresholds);
        free(context->groupAccessCounts);
        free(context->groupPullDecision);
        free(context);
        return NULL;
    }

    for(uint8_t g = 0; g < context->numGroups; ++g)
        context->groupThresholds[g] = context->fanout * calculateGroupHeight(g);

    logMessage("Push-pull context initialized", INFO);
    return context;
}

void updatePushPullContext(PushPullContext* context)
{
    logMessage("Updating push-pull context", DEBUG);

    if(!context)
    {
        logMessage("Null push-pull context provided", ERROR);
        return;
    }

    for(uint8_t g = 0; g < context->numGroups; ++g)
        context->groupPullDecision[g] = (context->groupAccessCounts[g] > context->groupThresholds[g]);

    logMessage("Push-pull context updated", INFO);
}

bool shouldPull(KDNode* node, uint32_t accessCount, uint8_t groupId, PushPullContext* context)
{
    logMessage("Evaluating pull decision", DEBUG);

    if(!node || !context)
    {
        logMessage("Null node or context provided", ERROR);
        return false;
    }

    if(context->forcePull)
    {
        logMessage("Force pull enabled, pulling", DEBUG);
        return true;
    }

    if(node->type == LEAF)
    {
        logMessage("Node is a leaf, skipping pull", DEBUG);
        return false;
    }

    bool decision = (groupId < context->numGroups) ? accessCount > context->groupThresholds[groupId] : accessCount > context->fanout * 4;

    char msg[64];
    snprintf(msg, sizeof(msg), "Pull decision for groupId=%u: %s", groupId, decision ? "pull" : "push");
    logMessage(msg, INFO);

    return decision;
}

void pushToPim(point** queries, uint32_t count, KDNode* startNode, uint8_t groupId, KDNode** results)
{
    logMessage("Pushing queries to PIM", DEBUG);

    if(count == 0 || !queries || !startNode)
    {
        logMessage("Invalid arguments for push to PIM", ERROR);
        return;
    }

    float** linearQueries = (float**)malloc(count * sizeof(float*));
    for(uint32_t i = 0; i < count; ++i)
        linearQueries[i] = queries[i]->coords;

    KDNode** tempResults = (KDNode**)calloc(count, sizeof(KDNode*));

    int ret = dispatchPushSearch(startNode, count, linearQueries, getDimensions(), groupId, tempResults);

    if(ret == 0)
    {
        for(uint32_t i = 0; i < count; ++i)
            if(tempResults[i])
                results[i] = tempResults[i];

        logMessage("Push to PIM completed", INFO);
    }
    else
        logMessage("Push search dispatch failed", ERROR);

    free(linearQueries);
    free(tempResults);
}

KDNode** pullToCpu(KDNode* node, point** queries, uint32_t count, PushPullContext* context)
{
    logMessage("Pulling node to CPU", DEBUG);

    if(!node || !queries || count == 0 || !context)
    {
        logMessage("Invalid arguments for pull to CPU", ERROR);
        return NULL;
    }

    float** linearQueries = (float**)malloc(count * sizeof(float*));
    for(uint32_t i = 0; i < count; ++i)
        linearQueries[i] = queries[i]->coords;

    KDNode** results = NULL;
    int ret = dispatchPullNode(node, count, linearQueries, getDimensions(), &results);

    free(linearQueries);

    if(ret != 0 || !results)
    {
        logMessage("Pull node dispatch failed", ERROR);
        return NULL;
    }

    logMessage("Pull to CPU completed", INFO);
    return results;
}

KDNode** searchGroup0(SearchBatch* batch)
{
    logMessage("Searching group 0", DEBUG);

    if(!batch || !batch->queries || batch->size == 0)
    {
        logMessage("Invalid batch provided to searchGroup0", ERROR);
        return NULL;
    }

    KDTree* tree = getData()->tree;
    if(!tree || !tree->root)
    {
        logMessage("Tree or root is null", ERROR);
        return NULL;
    }

    uint32_t nPim = getNPim();
    uint32_t dimensions = getDimensions();
    uint32_t queriesPerPim = batch->size / nPim;
    uint32_t remainder = batch->size % nPim;

    float*** queriesPerDpu = (float***)malloc(nPim * sizeof(float**));
    uint32_t* queriesCounts = (uint32_t*)calloc(nPim, sizeof(uint32_t));

    if(!queriesPerDpu || !queriesCounts)
    {
        logMessage("Failed to allocate per-DPU query arrays", ERROR);
        free(queriesPerDpu);
        free(queriesCounts);
        return NULL;
    }

    logMessage("Distributing queries across DPUs", DEBUG);

    for(uint32_t dpuId = 0; dpuId < nPim; ++dpuId)
    {
        uint32_t startIndex = dpuId * queriesPerPim + (dpuId < remainder ? dpuId : remainder);
        uint32_t localCount = queriesPerPim + (dpuId < remainder ? 1 : 0);

        if(localCount > 0 && startIndex < batch->size)
        {
            queriesCounts[dpuId] = localCount;
            queriesPerDpu[dpuId] = (float**)malloc(localCount * sizeof(float*));

            for(uint32_t i = 0; i < localCount && (startIndex + i) < batch->size; ++i)
                queriesPerDpu[dpuId][i] = batch->queries[startIndex + i]->coords;
        }
        else
        {
            queriesCounts[dpuId] = 0;
            queriesPerDpu[dpuId] = NULL;
        }
    }

    logMessage("Dispatching group 0 search", DEBUG);

    KDNode*** resultsPerDpu = NULL;
    int ret = dispatchGroup0Search(tree->root, queriesPerDpu, queriesCounts, nPim, dimensions, resultsPerDpu);

    if(ret != 0 || !resultsPerDpu)
    {
        logMessage("Group 0 search dispatch failed", ERROR);
        for(uint32_t i = 0; i < nPim; ++i)
            if(queriesPerDpu[i])
                free(queriesPerDpu[i]);

        free(queriesPerDpu);
        free(queriesCounts);
        return NULL;
    }

    KDNode** group1Roots = (KDNode**)calloc(batch->size, sizeof(KDNode*));
    if(!group1Roots)
    {
        logMessage("Failed to allocate group 1 roots", ERROR);
        for(uint32_t i = 0; i < nPim; ++i)
        {
            if(queriesPerDpu[i])
                free(queriesPerDpu[i]);

            if(resultsPerDpu[i])
                free(resultsPerDpu[i]);
        }

        free(queriesPerDpu);
        free(queriesCounts);
        free(resultsPerDpu);
        return NULL;
    }

    logMessage("Collecting results from DPUs", DEBUG);

    for(uint32_t dpuId = 0; dpuId < nPim; ++dpuId)
    {
        uint32_t startIndex = dpuId * queriesPerPim + (dpuId < remainder ? dpuId : remainder);
        uint32_t localCount = queriesCounts[dpuId];

        if(resultsPerDpu[dpuId] && localCount > 0)
            for(uint32_t i = 0; i < localCount && (startIndex + i) < batch->size; ++i)
                group1Roots[startIndex + i] = resultsPerDpu[dpuId][i];

        if(queriesPerDpu[dpuId])
            free(queriesPerDpu[dpuId]);

        if(resultsPerDpu[dpuId])
            free(resultsPerDpu[dpuId]);
    }

    logMessage("Resolving local replicas", DEBUG);

    KDNodeReplica** localReplicas = (KDNodeReplica**)calloc(batch->size, sizeof(KDNodeReplica*));
    if(localReplicas)
    {
        for(size_t i = 0; i < batch->size; ++i)
        {
            if(group1Roots[i])
            {
                KDGroup* group = findGroupForNode(group1Roots[i]);
                if(group && group->replicas)
                {
                    for(size_t j = 0; j < group->replicaCount; ++j)
                    {
                        if(group->replicas[j] && group->replicas[j]->masterNode == group1Roots[i])
                        {
                            localReplicas[i] = group->replicas[j];
                            break;
                        }
                    }
                }
            }
        }

        batch->localReplicas = localReplicas;
        logMessage("Local replicas resolved", INFO);
    }
    else
        logMessage("Failed to allocate local replicas, continuing without", ERROR);

    free(queriesPerDpu);
    free(queriesCounts);
    free(resultsPerDpu);

    logMessage("Group 0 search completed", INFO);
    return group1Roots;
}

void traverseGroupWithPushPull(SearchBatch* batch, KDNode** currentNodes, uint8_t groupId, PushPullContext* context)
{
    logMessage("Traversing group with push-pull", DEBUG);

    if(!batch || !currentNodes || !context)
    {
        logMessage("Null argument provided to traverseGroupWithPushPull", ERROR);
        return;
    }

    uint32_t* nodeCounts = (uint32_t*)calloc(batch->size, sizeof(uint32_t));
    uint32_t uniqueNodes = 0;

    for(size_t i = 0; i < batch->size; ++i)
    {
        if(!currentNodes[i] || currentNodes[i]->type == LEAF) continue;

        bool found = false;
        for(size_t j = 0; j < i; ++j)
        {
            if(currentNodes[j] == currentNodes[i])
            {
                nodeCounts[j]++;
                found = true;
                break;
            }
        }

        if(!found)
        {
            nodeCounts[i] = 1;
            uniqueNodes++;
        }
    }

    char msg[64];
    snprintf(msg, sizeof(msg), "Unique non-leaf nodes found: %u", uniqueNodes);
    logMessage(msg, INFO);

    if(uniqueNodes == 0)
    {
        logMessage("No unique non-leaf nodes, skipping traversal", DEBUG);
        free(nodeCounts);
        return;
    }

    NodeGroup* groups = (NodeGroup*)calloc(uniqueNodes, sizeof(NodeGroup));
    uint32_t groupIndex = 0;

    for(size_t i = 0; i < batch->size; ++i)
    {
        if(nodeCounts[i] > 0)
        {
            groups[groupIndex].node = currentNodes[i];
            groups[groupIndex].count = nodeCounts[i];
            groups[groupIndex].indices = (uint32_t*)malloc(nodeCounts[i] * sizeof(uint32_t));

            uint32_t index = 0;
            for(size_t j = i; j < batch->size; ++j)
                if(currentNodes[j] == currentNodes[i])
                    groups[groupIndex].indices[index++] = j;

            ++groupIndex;
        }
    }

    for(uint32_t g = 0; g < uniqueNodes; ++g)
        context->groupAccessCounts[groupId] += groups[g].count;

    #pragma omp parallel for
    for(uint32_t g = 0; g < uniqueNodes; ++g)
    {
        KDNode* node = groups[g].node;
        uint32_t accessCount = groups[g].count;

        point** nodeQueries = (point**)malloc(accessCount * sizeof(point*));
        for(uint32_t i = 0; i < accessCount; ++i)
            nodeQueries[i] = batch->queries[groups[g].indices[i]];

        KDNodeReplica* localReplica = NULL;
        if(batch->localReplicas)
        {
            for(size_t i = 0; i < batch->size; ++i)
            {
                if(batch->localReplicas[i] && batch->localReplicas[i]->masterNode == node)
                {
                    localReplica = batch->localReplicas[i];
                    break;
                }
            }
        }

        if(localReplica)
        {
            logMessage("Navigating using local replica", DEBUG);

            for(uint32_t i = 0; i < accessCount; ++i)
            {
                uint32_t index   = groups[g].indices[i];
                KDNode* nextNode = navigateUsingReplica(localReplica, nodeQueries[i], groupId);

                if(nextNode)
                {
                    currentNodes[index] = nextNode;
                    if(nextNode->type == LEAF)
                        batch->results[index] = nextNode;
                }
            }

            logMessage("Replica navigation completed", INFO);
        }
        else if(shouldPull(node, accessCount, groupId, context))
        {
            logMessage("Pulling subtree to CPU", DEBUG);

            KDNode** nextNodes = pullToCpu(node, nodeQueries, accessCount, context);
            if(nextNodes)
            {
                for(uint32_t i = 0; i < accessCount; ++i)
                {
                    uint32_t index      = groups[g].indices[i];
                    currentNodes[index] = nextNodes[i];

                    if(nextNodes[i] && nextNodes[i]->type == LEAF)
                        batch->results[index] = nextNodes[i];
                }

                free(nextNodes);
                logMessage("Pull to CPU traversal completed", INFO);
            }
            else
                logMessage("Pull to CPU returned null results", ERROR);
        }
        else
        {
            logMessage("Pushing queries to PIM", DEBUG);

            KDNode** pushResults = (KDNode**)calloc(accessCount, sizeof(KDNode*));
            pushToPim(nodeQueries, accessCount, node, groupId, pushResults);

            for(uint32_t i = 0; i < accessCount; ++i)
            {
                uint32_t index = groups[g].indices[i];
                if(pushResults[i])
                {
                    currentNodes[index]   = pushResults[i];
                    batch->results[index] = pushResults[i];
                }
            }

            free(pushResults);
            logMessage("Push to PIM traversal completed", INFO);
        }

        free(nodeQueries);
    }

    for(uint32_t g = 0; g < uniqueNodes; ++g)
        free(groups[g].indices);

    free(groups);
    free(nodeCounts);

    updatePushPullContext(context);
    logMessage("Group traversal completed", INFO);
}

SearchBatch* leafSearch(SearchBatch* batch)
{
    logMessage("Starting leaf search", DEBUG);

    if(!batch)
    {
        logMessage("Null batch provided to leaf search", ERROR);
        return NULL;
    }

    PushPullContext* context = initPushPullContext();
    if(!context)
    {
        logMessage("Failed to initialize push-pull context", ERROR);
        return batch;
    }

    KDNode** group1Roots = searchGroup0(batch);
    if(!group1Roots)
    {
        logMessage("Group 0 search returned null roots", ERROR);
        freePushPullContext(context);
        return batch;
    }

    KDNode** currentNodes = (KDNode**)malloc(batch->size * sizeof(KDNode*));
    if(!currentNodes)
    {
        logMessage("Failed to allocate current nodes array", ERROR);
        free(group1Roots);
        freePushPullContext(context);
        return batch;
    }

    for(size_t i = 0; i < batch->size; ++i)
        currentNodes[i] = group1Roots[i];

    for(uint8_t groupId = 1; groupId < context->numGroups; ++groupId)
    {
        bool allLeaves = true;
        for(size_t i = 0; i < batch->size && allLeaves; ++i)
            if(currentNodes[i] && currentNodes[i]->type != LEAF)
                allLeaves = false;

        if(allLeaves)
        {
            logMessage("All nodes are leaves, stopping traversal early", INFO);
            break;
        }

        traverseGroupWithPushPull(batch, currentNodes, groupId, context);
    }

    for(size_t i = 0; i < batch->size; ++i)
        if(currentNodes[i] && currentNodes[i]->type == LEAF && !batch->results[i])
            batch->results[i] = currentNodes[i];

    free(currentNodes);
    free(group1Roots);
    freePushPullContext(context);

    logMessage("Leaf search completed", INFO);
    return batch;
}

KDNode* navigateUsingReplica(KDNodeReplica* replica, point* q, uint8_t groupId)
{
    logMessage("Navigating using replica", DEBUG);

    if(!replica)
    {
        logMessage("Null replica provided", ERROR);
        return NULL;
    }

    KDNodeReplica* current = replica;

    while(current && current->type == INTERNAL)
        if(q->coords[current->data.internal.left->masterNode->data.internal.splitDim] < current->data.internal.left->masterNode->data.internal.splitValue)
            current = current->data.internal.left;
        else
            current = current->data.internal.right;

    if(current && current->masterNode)
        logMessage("Replica navigation successful", INFO);
    else
        logMessage("Replica navigation reached null node", ERROR);

    return current ? current->masterNode : NULL;
}

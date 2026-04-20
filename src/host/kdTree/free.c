#include <stdlib.h>

#include "host/kdTree/utils.h"
#include "host/kdTree/search.h"
#include "host/kdTree/distribute.h"
#include "host/kdTree/update.h"

#include "host/management/logging.h"

void freeKDTree(KDNode* node)
{
    logMessage("Freeing KD-tree node", DEBUG);

    if(!node)
        return;

    removeNodeFromMap(node);

    if(node->type == INTERNAL)
    {
        freeKDTree(node->data.internal.left);
        freeKDTree(node->data.internal.right);
    }
    else
    {
        if(node->data.leaf.points)
        {
            for(size_t i = 0; i < node->data.leaf.pointsCount; ++i)
                if(node->data.leaf.points[i].coords)
                    free(node->data.leaf.points[i].coords);

            free(node->data.leaf.points);
            node->data.leaf.points = NULL;
        }
    }

    free(node);
}

void freeMatrix(void** matrix, size_t rows)
{
    logMessage("Freeing matrix", DEBUG);

    if(!matrix)
    {
        logMessage("Null matrix provided, nothing to free", DEBUG);
        return;
    }

    for(size_t i = 0; i < rows; ++i)
        if(matrix[i])
            free(matrix[i]);

    free(matrix);

    char msg[256];
    snprintf(msg, sizeof(msg), "Matrix freed: %zu rows", rows);
    logMessage(msg, INFO);
}

void freeLeafNode(KDNode* node)
{
    logMessage("Freeing leaf node", DEBUG);

    if(!node)
        return;

    if(node->type == LEAF)
    {
        if(node->data.leaf.points)
            free(node->data.leaf.points);

        free(node);

        logMessage("Leaf node freed", INFO);
    }
    else
        logMessage("Node is not a leaf, skipping free", DEBUG);
}

void freeDpuAllocation(DpuAllocation* alloc)
{
    logMessage("Freeing DPU allocation", DEBUG);

    if(!alloc)
    {
        logMessage("Null DPU allocation provided, nothing to free", DEBUG);
        return;
    }

    free(alloc->nextOffset);
    free(alloc->allocationCount);
    free(alloc);

    logMessage("DPU allocation freed", INFO);
}

void freeSearchBatch(SearchBatch* batch)
{
    logMessage("Freeing search batch", DEBUG);

    if(!batch)
    {
        logMessage("Null search batch provided, nothing to free", DEBUG);
        return;
    }

    if(batch->results)
        free(batch->results);

    free(batch);

    logMessage("Search batch freed", INFO);
}

void freePushPullContext(PushPullContext* context)
{
    logMessage("Freeing push-pull context", DEBUG);

    if(!context)
    {
        logMessage("Null push-pull context provided, nothing to free", DEBUG);
        return;
    }

    if(context->groupThresholds)
        free(context->groupThresholds);

    if(context->groupAccessCounts)
        free(context->groupAccessCounts);

    if(context->groupPullDecision)
        free(context->groupPullDecision);

    free(context);

    logMessage("Push-pull context freed", INFO);
}

void freeReplica(KDNodeReplica* replica)
{
    logMessage("Freeing replica node", DEBUG);

    if(!replica)
        return;

    if(replica->type == INTERNAL)
    {
        if(replica->data.internal.left)
            freeReplica(replica->data.internal.left);

        if(replica->data.internal.right)
            freeReplica(replica->data.internal.right);
    }
    else
    {
        if(replica->data.leaf.points)
            free(replica->data.leaf.points);
    }

    if(replica->descendants)
        free(replica->descendants);

    if(replica->ancestors)
        free(replica->ancestors);

    free(replica);
}

void freeNodeLocationMap()
{
    logMessage("Freeing node location map", DEBUG);

    Data* data = getData();

    if(!data->map)
    {
        logMessage("Node location map already null, nothing to free", DEBUG);
        return;
    }

    free(data->map->nodes);
    free(data->map->dpuIds);
    free(data->map->dpuAddresses);
    free(data->map);
    data->map = NULL;

    logMessage("Node location map freed", INFO);
}

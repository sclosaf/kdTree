#include <stdlib.h>

#include "kdTree/free.h"

void freeKDTree(KDNode* node)
{
    if(!node)
        return;

    if(node->type == INTERNAL)
    {
        freeKDTree(node->data.internal.left);
        freeKDTree(node->data.internal.right);
    }
    else
    {
        if(node->data.leaf.points)
        {
            free(node->data.leaf.points);
            node->data.leaf.points = NULL;
        }
    }

    free(node);
}

void freeMatrix(void** matrix, size_t rows)
{
    if(!matrix)
        return;

    for(size_t i = 0; i < rows; ++i)
    {
        if(matrix[i])
            free(matrix[i]);
    }
    free(matrix);
}

void freeLeafNode(KDNode* node)
{
    if(!node)
        return;

    if(node->type == LEAF)
    {
        if (node->data.leaf.points)
            free(node->data.leaf.points);

        free(node);
    }
}

void freeDpuAllocation(DpuAllocation* alloc)
{
    if(alloc)
    {
        free(alloc->nextOffset);
        free(alloc->allocationCount);
        free(alloc);
    }
}

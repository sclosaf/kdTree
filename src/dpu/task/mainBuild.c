#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <alloc.h>
#include <mram.h>
#include <attributes.h>
#include <string.h>
#include <mram_unaligned.h>

#include "dpu/kdTree/build.h"
#include "dpu/kdTree/serialization.h"
#include "dpu/kdTree/utils.h"
#include "dpu/environment/config.h"
#include "dpu/environment/macro.h"
#include "dpu/kdTree/types.h"

#define MAX_POINTS_SIZE (8 * 1024 * 1024)
#define MAX_TREE_SIZE   (16 * 1024 * 1024)
#define MAX_MAP_ENTRIES (1024)

__mram_noinit uint8_t MRAM_INPUT_BUFFER[MAX_POINTS_SIZE];
__mram_noinit uint8_t MRAM_OUTPUT_BUFFER[MAX_TREE_SIZE];
__mram_noinit uint64_t MRAM_NODE_MAP[MAX_MAP_ENTRIES];

__host uint32_t nPoints;
__host uint32_t outputSize;
__host uint32_t mapSize;

int main()
{
    mem_reset();

    uint32_t dims = (uint32_t)getDimensions();

    if(nPoints == 0)
    {
        DPU_LOG("DPU: No points to process");
        return 0;
    }

    DPU_LOG("DPU: Starting Build - Points: %u", nPoints);

    size_t pointsByteSize = (size_t)nPoints * dims * sizeof(float);
    size_t alignedPointsSize = (pointsByteSize + 7) & ~7;

    if(alignedPointsSize > MAX_POINTS_SIZE)
    {
        DPU_LOG("DPU: ERR - Input size (%u) exceeds MAX_POINTS_SIZE", (uint32_t)alignedPointsSize);
        return 1;
    }


    float* localPoints = (float*)mem_alloc(alignedPointsSize);
    if(localPoints == NULL)
    {
        DPU_LOG("DPU: ERR - mem_alloc failed for localPoints");
        return 1;
    }

    mram_read(MRAM_INPUT_BUFFER, localPoints, alignedPointsSize);

    point* pointsPool = (point*)mem_alloc(nPoints * sizeof(point));
    point** points = (point**)mem_alloc(nPoints * sizeof(point*));

    if(pointsPool == NULL || points == NULL)
    {
        DPU_LOG("DPU: ERR - Out of WRAM for points pointers");
        return 1;
    }

    for(size_t i = 0; i < nPoints; ++i)
    {
        points[i] = &pointsPool[i];
        points[i]->coords = localPoints + (i*dims);
    }

    DPU_LOG("DPU: Building tree on-chip...");
    KDNode* root = buildOnChip(points, nPoints);
    if(root == NULL)
    {
        DPU_LOG("DPU: ERR - buildOnChip returned NULL");
        return 1;
    }

    DPU_LOG("DPU: Serializing tree...");
    size_t tempOutSize;
    uint8_t* tempOut = serializeTree(root, &tempOutSize);

    if(tempOut != NULL && tempOutSize > 0)
    {
        if(tempOutSize > MAX_TREE_SIZE)
        {
            DPU_LOG("DPU: ERR - Serialized tree exceeds MAX_TREE_SIZE");
            return 1;
        }

        outputSize = (uint32_t)tempOutSize;
        size_t alignedOutSize = (tempOutSize + 7) & ~7;

        mram_write(tempOut, MRAM_OUTPUT_BUFFER, alignedOutSize);

        DPU_LOG("DPU: Tree serialized, size: %u bytes at MRAM_OUTPUT_BUFFER", outputSize);
    }
    else
    {
        outputSize = 0;
        DPU_LOG("DPU: ERR - Serialization failed");
        return 1;
    }

    DPU_LOG("DPU: Building address map...");
    size_t tempMapSize;
    uint64_t* tempMap = buildNodeAddressMap(root, &tempMapSize);

    if(tempMap != NULL && tempMapSize > 0)
    {
        if(tempMapSize > MAX_MAP_ENTRIES)
        {
            DPU_LOG("DPU: ERR - Map entries exceed MAX_MAP_ENTRIES");
            return 1;
        }

        mapSize = (uint32_t)tempMapSize;
        size_t mapByteSize = tempMapSize * sizeof(uint64_t);
        size_t alignedMapSize = (mapByteSize + 7) & ~7;

        mram_write(tempMap, MRAM_NODE_MAP, alignedMapSize);

        DPU_LOG("DPU: Map built: %zu entries at MRAM_NODE_MAP", tempMapSize);
    }
    else
    {
        mapSize = 0;
        DPU_LOG("DPU: Map build failed or empty");
    }

    DPU_LOG("DPU: Task completed successfully");
    return 0;
}

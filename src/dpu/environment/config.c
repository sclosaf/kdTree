#include <attributes.h>

#include "dpu/environment/config.h"

__host DpuConfig dpuConfig;

uint32_t getNPoint()
{
    return dpuConfig.nPoint;
}

uint8_t getNPim()
{
    return dpuConfig.nPim;
}

uint8_t getDimensions()
{
    return dpuConfig.dimensions;
}

float getAlpha()
{
    return dpuConfig.alpha;
}

float getBeta()
{
    return dpuConfig.beta;
}

float getMinCoord()
{
    return dpuConfig.minCoord;
}

float getMaxCoord()
{
    return dpuConfig.maxCoord;
}

uint16_t getLeafWrapThreshold()
{
    return dpuConfig.leafWrapThreshold;
}

uint16_t getOversamplingRate()
{
    return dpuConfig.oversamplingRate;
}

uint8_t getSketchHeight()
{
    return dpuConfig.sketchHeight;
}

uint16_t getChunkSize()
{
    return dpuConfig.chunkSize;
}

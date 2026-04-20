#ifndef DPU_ENVIRONMENT_CONFIG_H
#define DPU_ENVIRONMENT_CONFIG_H

#include <stdint.h>

typedef struct DpuConfig
{
    uint32_t nPoint;
    float alpha;
    float beta;
    float minCoord;
    float maxCoord;
    uint16_t leafWrapThreshold;
    uint16_t oversamplingRate;
    uint16_t chunkSize;
    uint8_t nPim;
    uint8_t dimensions;
    uint8_t sketchHeight;
    uint8_t padding;
} __attribute__((aligned(8))) DpuConfig;

uint32_t getNPoint();
uint8_t getNPim();
uint8_t getDimensions();
float getAlpha();
float getBeta();
float getMinCoord();
float getMaxCoord();
uint16_t getLeafWrapThreshold();
uint16_t getOversamplingRate();
uint8_t getSketchHeight();
uint16_t getChunkSize();

#endif

#ifndef LOADER_CONFIG_H
#define LOADER_CONFIG_H

#include <dpu.h>
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

void sendConfigToDpu(struct dpu_set_t dpu);

#endif

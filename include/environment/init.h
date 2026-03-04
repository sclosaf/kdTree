#ifndef ENVIRONMENT_INIT_H
#define ENVIRONMENT_INIT_H

typedef struct Config
{
    uint32_t nPoint;
    uint8_t nPim;
    uint8_t dimensions;
    float alpha;
    float beta;
    uint16_t leafWrapThreshold;
    uint16_t overSamplingRate;
    uint8_t sketchHeight;
    uint16_t chunkSize;
} Config;

const Config* getConfig();
void initConfig();
void resetConfig();
void printConfig();

#endif

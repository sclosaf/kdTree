#ifndef ENVIRONMENT_INIT_H
#define ENVIRONMENT_INIT_H

#include <stdint.h>

typedef struct Config
{
    uint32_t nPoint;
    uint8_t nPim;
    uint8_t dimensions;
    float alpha;
    float beta;
    uint16_t leafWrapThreshold;
    uint16_t oversamplingRate;
    uint8_t sketchHeight;
    uint16_t chunkSize;
} Config;

const Config* getConfig();
void initConfig();
void resetConfig();
void printConfig();

char* getProjectRoot()

#endif

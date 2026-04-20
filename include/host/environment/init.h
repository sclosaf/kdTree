#ifndef ENVIRONMENT_INIT_H
#define ENVIRONMENT_INIT_H

#include <stdint.h>

#include "host/kdTree/types.h"
#include "host/kdTree/build.h"
#include "host/kdTree/distribute.h"

typedef struct Config
{
    uint32_t nPoint;
    float minCoord;
    float maxCoord;
    uint8_t nPim;
    uint8_t dimensions;
    float alpha;
    float beta;
    uint16_t leafWrapThreshold;
    uint16_t oversamplingRate;
    uint8_t sketchHeight;
    uint16_t chunkSize;
} Config;

typedef struct Data
{
    KDTree* tree;
    NodeLocationMap* map;
} Data;

uint32_t getNPoint();
float getMinCoord();
float getMaxCoord();
uint8_t getNPim();
uint8_t getDimensions();
float getAlpha();
float getBeta();
uint16_t getLeafWrapThreshold();
uint16_t getOversamplingRate();
uint8_t getSketchHeight();
uint16_t getChunkSize();

void setNPoint(uint32_t value);
void setMinCoord(float value);
void setMaxCoord(float value);
void setNPim(uint8_t value);
void setDimensions(uint8_t value);
void setAlpha(float value);
void setBeta(float value);
void setLeafWrapThreshold(uint16_t value);
void setOversamplingRate(uint16_t value);
void setSketchHeight(uint8_t value);
void setChunkSize(uint16_t value);

Data* getData();
void freeData();

void initConfig();
void resetConfig();
void printConfig();

char* getProjectRoot();

#endif

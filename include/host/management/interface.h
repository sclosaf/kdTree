#ifndef MANAGEMENT_INTERFACE_H
#define MANAGEMENT_INTERFACE_H

#define FLAG_TERMINATOR {NULL, NULL, NULL, NULL, 0, false, NULL}

#include <stdint.h>
#include <stdbool.h>

#include "host/kdTree/types.h"
#include "host/management/reader.h"

typedef enum CommandType
{
    BUILD,
    INSERT,
    DELETE,
    KNN,
    RANGE,
    CLUSTER_DPC,
    CLUSTER_DBSCAN,
    BENCHMARKS,
    INFOS,
    CONFIG,
    SET,
    QUIT,
    UNKNOWN
} CommandType;

typedef enum BuildType
{
    CHIP,
    PIM
} BuildType;

typedef struct BuildContext
{
    BuildType type;
    Dataset dataset;
} BuildContext;

typedef enum InsertType
{
    INSERT_FROM_FILE,
    INSERT_RANDOM,
    INSERT_COORDINATES
} InsertType;

typedef struct InsertContext
{
    InsertType source;
    uint32_t offsetMultiplier;
    Dataset datasetType;
    uint32_t count;
    point* point;
} InsertContext;

typedef enum DeleteType
{
    DELETE_COORDINATES,
    DELETE_RANDOM,
    DELETE_LEAF
} DeleteType;

typedef struct DeleteContext
{
    DeleteType type;
    point** point;
    uint32_t count;
    uint32_t leafCount;
} DeleteContext;

typedef enum KnnSource
{
    KNN_RANDOM,
    KNN_COORDINATES
} KnnSource;

typedef struct KNNContext
{
    point** queries;
    uint32_t queryCount;
    uint32_t k;
    bool approximate;
    float epsilon;
    KnnSource source;
    Dataset datasetType;
} KNNContext;

typedef enum RangeSource
{
    RANGE_RANDOM,
    RANGE_COORDINATES
} RangeSource;

typedef struct RangeContext
{
    float** queries;
    uint32_t queryCount;
    RangeSource source;
    Dataset datasetType;
} RangeContext;

typedef struct DPCContext
{
    uint32_t kNeighbors;
    float minDensityThreshold;
    float minDeltaThreshold;
} DPCContext;

typedef struct DBSCANContext
{
    float epsilon;
    uint32_t minPts;
    float gridCellSize;
} DBSCANContext;

typedef enum ConfigType
{
    INIT,
    RESET,
    CONFIGURATION,
    SPECIFICS,
    DATASET
} ConfigType;

typedef struct ConfigContext
{
    ConfigType type;
    Dataset dataset;
} ConfigContext;

typedef enum SetType
{
    NPOINT,
    MINCOORD,
    MAXCOORD,
    NPIM,
    DIMENSIONS,
    ALPHA,
    BETA,
    LEAFWRAPTHRESHOLD,
    OVERSAMPLINGRATE,
    SKETCHHEIGHT,
    CHUNKSIZE
} SetType;

typedef struct SetContext
{
    SetType type;
    void* value;
} SetContext;

typedef struct FlagDefinition
{
    const char* shortFlag;
    const char* longFlag;
    const char* description;
    const char* argName;
    uint8_t argCount;
    bool required;
    bool (*validator)(char** args, uint8_t argCount);
} FlagDefinition;

typedef struct CommandParser
{
    CommandType type;
    void* (*parse)(char** argv, int argc);
    void (*free)(void* context);
} CommandParser;

typedef struct CommandHandler
{
    CommandType type;
    const char* longName;
    const char* shortName;
    const char* description;
    FlagDefinition* flags;
    uint8_t flagCount;
    CommandParser* parser;
    int (*handler)(void* context);
} CommandHandler;

typedef struct CommandRegistry
{
    CommandHandler* handlers;
    uint8_t count;
} CommandRegistry;

void run();

void initCommandRegistry();
void freeCommandRegistry();

void printHelp();
void printCommandHelp();
void printAvailableCommands();

int processCommand(CommandType type, char* line);

bool validatePositiveInt(char** args, uint8_t argCount);
bool validateNonNegativeInt(char** args, uint8_t argCount);
bool validateFloat(char** args, uint8_t argCount);
bool validateDataset(char** args, uint8_t argCount);
bool validateRangeCoordinates(char** args, uint8_t argCount);
bool validateCoordinates(char** args, uint8_t argCount);

FlagDefinition* getBuildFlags();
FlagDefinition* getInsertFlags();
FlagDefinition* getDeleteFlags();
FlagDefinition* getKnnFlags();
FlagDefinition* getRangeFlags();
FlagDefinition* getDpcFlags();
FlagDefinition* getDbscanFlags();
FlagDefinition* getInfoFlags();
FlagDefinition* getConfigFlags();
FlagDefinition* getSetFlags();

int handleBuild(void* context);
int handleInsert(void* context);
int handleDelete(void* context);
int handleKNN(void* context);
int handleRange(void* context);
int handleClusterDPC(void* context);
int handleClusterDBSCAN(void* context);
int handleBenchmark(void* context);
int handleInfo(void* context);
int handleConfig(void* context);
int handleSet(void* context);

extern CommandParser buildParser;
extern CommandParser insertParser;
extern CommandParser deleteParser;
extern CommandParser infoParser;
extern CommandParser configParser;
extern CommandParser setParser;
extern CommandParser knnParser;
extern CommandParser rangeParser;
extern CommandParser dpcParser;
extern CommandParser dbscanParser;
extern CommandParser benchmarkParser;

#endif

#ifndef MANAGEMENT_INTERFACE_H
#define MANAGEMENT_INTERFACE_H

#include <stdint.h>
#include <stdbool.h>

#include "kdTree/types.h"
#include "management/dpuManagement.h"

typedef enum CommandType
{
    BUILD,
    INSERT,
    DELETE,
    KNN,
    RANGE,
    CLUSTER_DPC,
    CLUSTER_DBSCAN,
    TEST,
    BENCHMARK,
    INFO,
    VALIDATE,
    CONFIG,
    QUIT,
    UNKNOWN
} CommandType;

typedef struct CommandHandler
{
    CommandType type;
    const char* longName;
    const char* shortName;
    const char* description;
    const char* subFlag;
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
void printAvailableCommands();

int processCommand(CommandType type, char* line);

int handleBuild(void* context);
int handleInsert(void* context);
int handleDelete(void* context);
int handleKNN(void* context);
int handleRange(void* context);
int handleClusterDPC(void* context);
int handleClusterDBSCAN(void* context);
int handleTest(void* context);
int handleBenchmark(void* context);
int handleInfo(void* context);
int handleValidate(void* context);
int handleConfig(void* context);

#endif

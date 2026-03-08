#ifndef MANAGEMENT_INTERFACE_H
#define MANAGEMENT_INTERFACE_H

#include <stdint.h>
#include <stdbool.h>

#include "kdTree/types.h"
#include "management/dpuManagement.h"

typedef enum CommandType
{
    BUILD,
    QUERY,
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
    int (*handler)();
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

int executeCommand(CommandType cmd);

int handleBuild();
int handleQuery();
int handleInsert();
int handleDelete();
int handleKNN();
int handleRange();
int handleClusterDPC();
int handleClusterDBSCAN();
int handleTest();
int handleBenchmark();
int handleInfo();
int handleValidate();
int handleConfig();

#endif

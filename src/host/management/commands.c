#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>

#include "management/interface.h"
#include "management/logging.h"
#include "environment/init.h"

static CommandRegistry* registry = NULL;

static CommandType getCommandType(char* cmd)
{
    if(!cmd)
        return UNKNOWN;

    for(size_t i = 0; i < strlen(cmd); ++i)
        cmd[i] = tolower(cmd[i]);

    for(size_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* handler = &registry->handlers[i];

        if(handler->longName && strcmp(cmd, handler->longName) == 0)
            return handler->type;

        if(handler->shortName && strcmp(cmd, handler->shortName) == 0)
            return handler->type;
    }

    return UNKNOWN;
}

void run()
{
    if(!registry)
        initCommandRegistry();

    char line[128];
    bool running = true;

    printf("Type 'help' for available commands, 'quit' to exit\n");

    while(running)
    {
        printf("$ ");
        fflush(stdout);

        if(!fgets(line, sizeof(line), stdin))
            break;

        line[strcspn(line, "\n")] = 0;

        if(strlen(line) == 0)
            continue;

        if(strcmp(line, "help") == 0 || strcmp(line, "h") == 0)
        {
            printHelp();
            continue;
        }

        if(strcmp(line, "quit") == 0 || strcmp(line, "q") == 0)
        {
            printf("Quitting\n");
            running = false;
            break;
        }

        CommandType type = getCommandType(line);
        int result = executeCommand(type);

        if(result != 0)
            printf("Command execution failed with code %d\n", result);
    }

    freeCommandRegistry();
}

void initCommandRegistry()
{
    registry = (CommandRegistry*)malloc(sizeof(CommandRegistry));
    if(!registry)
        return;

    registry->handlers = NULL;
    registry->count = 0;

    CommandHandler defaultHandlers[] =
    {
        {BUILD, "build", "b", "Build a new PIM-kd-tree", handleBuild},
        {QUERY, "query", "que", "Execute point queries", handleQuery},
        {INSERT, "insert", "i", "Insert points into tree", handleInsert},
        {DELETE, "delete", "d", "Delete points from tree", handleDelete},
        {KNN, "knn", "k", "Execute k-nearest neighbor queries", handleKNN},
        {RANGE, "range", "r", "Execute orthogonal range query", handleRange},
        {CLUSTER_DPC, "dpc", "dp", "Run Density Peak Clustering", handleClusterDPC},
        {CLUSTER_DBSCAN, "dbscan", "db", "Run DBSCAN clustering", handleClusterDBSCAN},
        {TEST, "test", "t", "Run test suite", handleTest},
        {BENCHMARK, "bench", "be", "Run benchmarks", handleBenchmark},
        {INFO, "info", "inf", "Display tree information", handleInfo},
        {VALIDATE, "validate", "v", "Validate tree structure", handleValidate},
        {CONFIG, "config", "c", "Configure system parameters", handleConfig},
    };

    uint8_t numHandlers = sizeof(defaultHandlers) / sizeof(defaultHandlers[0]);

    // Alloca un array di handler invece di lista concatenata
    registry->handlers = (CommandHandler*)malloc(numHandlers * sizeof(CommandHandler));
    if(registry->handlers)
    {
        memcpy(registry->handlers, defaultHandlers, numHandlers * sizeof(CommandHandler));
        registry->count = numHandlers;
    }
}

void freeCommandRegistry()
{
    if(!registry)
        return;

    free(registry->handlers);
    free(registry);
    registry = NULL;
}

void printHelp()
{
    if(!registry)
        return;

    printf("Usage: $ <command>\n\n");
    printf("Available commands:\n");

    printAvailableCommands();

    printf("  help      (short: h) - Show this help\n");
    printf("  quit      (short: q) - Exit the program\n");
}

void printAvailableCommands()
{
    if(!registry || !registry->handlers)
        return;

    for(uint8_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* h = &registry->handlers[i];
        printf("  %-12s", h->longName);

        if(h->shortName)
            printf(" (short: %s)", h->shortName);
        else
            printf("             ");

        printf(" - %s\n", h->description);
    }
}

int executeCommand(CommandType cmd)
{
    if(!registry)
        return -1;

    for(uint8_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* h = &registry->handlers[i];
        if(h->type == cmd)
            return h->handler();
    }

    printf("Unknown command\n");
    return -1;
}

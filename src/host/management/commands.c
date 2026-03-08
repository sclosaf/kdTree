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
        int result = processCommand(type, line);

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
        {BUILD, "build", "b", "Build a new PIM-kd-tree", NULL, handleBuild},
        {QUERY, "query", "que", "Execute point queries", NULL, handleQuery},
        {INSERT, "insert", "i", "Insert points into tree", NULL, handleInsert},
        {DELETE, "delete", "d", "Delete points from tree", NULL, handleDelete},
        {KNN, "knn", "k", "Execute k-nearest neighbor queries", NULL, handleKNN},
        {RANGE, "range", "r", "Execute orthogonal range query", NULL, handleRange},
        {CLUSTER_DPC, "dpc", "dp", "Run Density Peak Clustering", NULL, handleClusterDPC},
        {CLUSTER_DBSCAN, "dbscan", "db", "Run DBSCAN clustering", NULL, handleClusterDBSCAN},
        {TEST, "test", "t", "Run test suite", NULL, handleTest},
        {BENCHMARK, "bench", "be", "Run benchmarks", NULL, handleBenchmark},
        {INFO, "info", "inf", "Display tree information", "-c|-d|-t", handleInfo},
        {VALIDATE, "validate", "v", "Validate tree structure", NULL, handleValidate},
        {CONFIG, "config", "c", "Configure system parameters", NULL, handleConfig},
    };

    uint8_t numHandlers = sizeof(defaultHandlers) / sizeof(defaultHandlers[0]);

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

int parseCommand(CommandType type, char* line)
{
    if(!line || strlen(line) == 0)
    {
        if(error) *error = strdup("Empty command");
        return -1;
    }

    char* cmdStr = strtok(line, " \t");
    if(!cmdStr)
    {
        free(line);
        return -1;
    }

    char* argv[64];
    int argc = 0;

    argv[argc++] = cmdStr;

    char* token = strtok(NULL, " \t");
    while(token && argc < 64)
    {
        argv[argc++] = token;
        token = strtok(NULL, " \t");
    }

    int i = 1;

    switch(type)
    {
        case BUILD:
            return 0;

        case QUERY:
            return 0;

        case INSERT:
            return 0;

        case DELETE:
            return 0;

        case KNN:
            return 0;

        case RANGE:
            return 0;

        case CLUSTER_DPC:
            return 0;

        case CLUSTER_DBSCAN:
            return 0;

        case TEST:
            return 0;

        case BENCHMARK:
            return 0;

        case INFO:
            if(argc >= 2)
                return -1;

            Style* style = (Style*)malloc(sizeof(Style));

            if(strcmp(argv[i], "-c") == 0)
                *style = COMPACT;
            else if(strcmp(argv[i], "-d") == 0)
                *style = DETAILED;
            else if(strcmp(argv[i], "-t") == 0)
                *style = TREE;
            else
                return -1;

            CommandHandler* h = &registry->handlers[type];
            return h->handler(style);

        case VALIDATE:
            CommandHandler* h = &registry->handlers[type];
            return h->handler(NULL);

        case CONFIG:
            CommandHandler* h = &registry->handlers[type]; // Constants and specifics
            return h->handler(NULL);

        default:
            return -1;
    }
}

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>

#include "kdTree/print.h"

#include "management/interface.h"
#include "management/logging.h"

#include "environment/init.h"

static CommandRegistry* registry = NULL;

static CommandType getCommandType(char* cmd)
{
    if(!cmd)
        return UNKNOWN;

    char* token = strtok(cmd, " \t");
    if(!token)
        return UNKNOWN;

    for(size_t i = 0; i < strlen(token); ++i)
        token[i] = tolower(token[i]);

    for(size_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* handler = &registry->handlers[i];

        if(handler->longName && strcmp(token, handler->longName) == 0)
            return handler->type;

        if(handler->shortName && strcmp(token, handler->shortName) == 0)
            return handler->type;
    }

    return UNKNOWN;
}

void run()
{
    if(!registry)
        initCommandRegistry();

    char line[128];
    char copy[128];
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

        strcpy(copy, line);

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
        int result = processCommand(type, copy);

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
        {INSERT, "insert", "i", "Insert points into tree", NULL, handleInsert},
        {DELETE, "delete", "d", "Delete points from tree", NULL, handleDelete},
        {KNN, "knn", "k", "Execute k-nearest neighbor queries", NULL, handleKNN},
        {RANGE, "range", "r", "Execute orthogonal range query", NULL, handleRange},
        {CLUSTER_DPC, "dpc", "dp", "Run Density Peak Clustering", NULL, handleClusterDPC},
        {CLUSTER_DBSCAN, "dbscan", "db", "Run DBSCAN clustering", NULL, handleClusterDBSCAN},
        {TEST, "test", "t", "Run test suite", NULL, handleTest},
        {BENCHMARK, "bench", "be", "Run benchmarks", NULL, handleBenchmark},
        {INFO, "info", "inf", "Display tree information", "[-c|-t|-d|-v|-a|-s|-m|-o <id>|-f]", handleInfo},
        {CONFIG, "config", "c", "Configure system parameters", "[-i|-r|-p|-s]", handleConfig},
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

    printf("- help / h \t\t\t\tShow this help\n");
    printf("- quit / q \t\t\t\tExit the program\n");
}

void printAvailableCommands()
{
    if(!registry || !registry->handlers)
        return;

    for(uint8_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* h = &registry->handlers[i];

        printf("- %s", h->longName);

        if(h->shortName)
            printf(" / %s", h->shortName);

        if(h->subFlag)
            printf(" %s", h->subFlag);

        int len = strlen(h->longName) + 2;
        if(h->shortName)
            len += strlen(h->shortName) + 3;
        if(h->subFlag)
            len += strlen(h->subFlag) + 1;

        int tabs = (40 - len + 7) / 8;
        if(tabs < 1)
            tabs = 1;

        for(int t = 0; t < tabs; t++)
            printf("\t");

        printf("%s\n", h->description);
    }
}

int processCommand(CommandType type, char* line)
{
    if(!line || strlen(line) == 0)
    {
        return -1;
    }

    char* cmdStr = strtok(line, " \t");
    if(!cmdStr)
        return -1;

    char* argv[8] = {NULL};
    int argc = 0;

    argv[argc++] = cmdStr;

    char* token = strtok(NULL, " \t");
    while(token && argc < 8)
    {
        argv[argc++] = token;
        token = strtok(NULL, " \t");
    }

    int i = 1;
    CommandHandler* h = NULL;

    switch(type)
    {
        case BUILD:
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
            PrintOptions* opt =
            {
                .style = COMPACT,
                .dpuId = 0
            };

            if(strcmp(argv[1], "-c") == 0)
                opt->style =  COMPACT;
            else if(strcmp(argv[1], "-t") == 0)
                opt->style = TREE;
            else if(strcmp(argv[1], "-d") == 0)
                opt->style = DETAILED;
            else if(strcmp(argv[1], "-v") == 0)
                opt->style = VALIDATE;
            else if(strcmp(argv[1], "-a") == 0)
                opt->style = APPROXIMATE;
            else if(strcmp(argv[1], "-s") == 0)
                opt->style = STATS;
            else if(strcmp(argv[1], "-m") == 0)
                opt->style = MEMORY;
            else if(strcmp(argv[1], "-o") == 0)
            {
                if(argc != 2 || argv[2] == NULL)
                    return -1;

                uint32_t id = atoi(argv[i+1]);

                if(id < 0)
                    return -1;

                opt->style = ONDPU;
                opt->dpuId = (uint32_t)id;
            }
            else if(strcmp(flag, "-f") == 0)
                opt->style = FULL;

            h = &registry->handlers[type];
            return h->handler(opt);

        case CONFIG:
            ConfigContext* ctx = (ConfigContext*)malloc(sizeof(ConfigContext));
            *ctx = PRINT;

            if(strcmp(argv[1], "-i") == 0)
                *ctx = INIT;
            else if(strcmp(argv[1], "-r") == 0)
                *ctx = RESET;
            else if(strcmp(argv[1], "-p") == 0)
                *ctx = PRINT;
            else if(strcmp(argv[1], "-s") == 0)
                *ctx = SPECIFICS;

            h = &registry->handlers[type];
            return h->handler(ctx);

        default:
            return -1;
    }
}

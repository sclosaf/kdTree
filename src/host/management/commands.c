#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>

#include "cli/commands.h"
#include "management/logging.h"
#include "environment/init.h"

static CommandRegistry* registry = NULL;

static CommandType getCommandType(const char* cmd)
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

static char* parseString(const char* arg)
{
    if(!arg)
        return NULL;
    return strdup(arg);
}

static uint32_t parseUint32(const char* arg, bool* success)
{
    *success = false;
    if(!arg)
        return 0;

    char* endptr;
    long val = strtol(arg, &endptr, 10);

    if(*endptr == '\0' && val >= 0 && val <= UINT32_MAX)
    {
        *success = true;
        return (uint32_t)val;
    }

    return 0;
}

static float parseFloat(const char* arg, bool* success)
{
    *success = false;
    if(!arg)
        return 0.0f;

    char* endptr;
    float val = strtof(arg, &endptr);

    if(*endptr == '\0')
    {
        *success = true;
        return val;
    }

    return 0.0f;
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

        char* error = NULL;
        Command* cmd = parseCommand(line, &error);

        if(!cmd)
        {
            if(error)
            {
                printf("Error: %s\n", error);
                free(error);
            }

            continue;
        }

        int result = executeCommand(cmd);
        if(result != 0)
            printf("Command execution failed with code %d\n", result);
    }

    freeCommandRegistry();
}

void initCommandRegistry()
{
    CommandRegistry* registry = (CommandRegistry*)malloc(sizeof(CommandRegistry));
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

    for(size_t i = 0; i < sizeof(defaultHandlers) / sizeof(defaultHandlers[0]); ++i)
    {
        CommandHandler* handler = (CommandHandler*)malloc(sizeof(CommandHandler));
        if(handler)
        {
            memcpy(handler, &defaultHandlers[i], sizeof(CommandHandler));
            handler->next = registry->handlers;
            registry->handlers = handler;
            ++registry->count;
        }
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

void printHelp(void)
{
    if(!registry)
        return;

    printf("Usage: $<command> [options]\n\n");
    printf("Available commands:\n");

    printAvailableCommands();
}

void printAvailableCommands()
{
    if(!registry || !registry->handlers)
        return;

    for(size_t i = 0; i < registry->count; ++i)
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

Command* parseCommand(const char* line, char** error)
{
    if(!line || strlen(line) == 0)
    {
        if(error)
            *error = strdup("Empty command");

        return NULL;
    }

    char* lineCopy = strdup(line);
    if(!lineCopy)
    {
        if(error)
            *error = strdup("Memory allocation failed");
        return NULL;
    }

    char* cmdStr = strtok(lineCopy, " \t");
    if(!cmdStr)
    {
        free(lineCopy);
        if(error)
            *error = strdup("No command specified");
        return NULL;
    }

    CommandType type = getCommandType(cmdStr);

    if(cmd->type == UNKNOWN)
    {
        free(lineCopy);
        free(cmd);
        if(error)
        {
            *error = malloc(128);
            snprintf(*error, 128, "Unknown command: %s", cmdStr);
        }

        return NULL;
    }

    char* argv[8];
    int argc = 0;

    argv[argc++] = cmdStr;

    char* token = strtok(NULL, " \t");
    while(token && argc < 8)
    {
        argv[argc++] = token;
        token = strtok(NULL, " \t");
    }

    int i = 1;
    bool success = false;

    switch(type) // TO COMPLETE
    {
        case BUILD:
            while(i < argc)
                ++i;
            break;

        case QUERY:
            while(i < argc)
                ++i;
            break;

        case INSERT:
            while(i < argc)
                ++i;
            break;

        case DELETE:
            while(i < argc)
                ++i;
            break;

        case KNN:
            while(i < argc)
                ++i;
            break;

        case RANGE:
            while(i < argc)
                ++i;
            break;

        case CLUSTER_DPC:
            while(i < argc)
                ++i;
            break;

        case CLUSTER_DBSCAN:
            while(i < argc)
                ++i;
            break;

        case TEST:
            while(i < argc)
                ++i;
            break;

        case BENCHMARK:
            while(i < argc)
                ++i;
            break;

        case INFO:
            while(i < argc)
                ++i;
            break;

        case VALIDATE:
            while(i < argc)
                ++i;
            break;

        case CONFIG:
            while(i < argc)
                ++i;
            break;

        default:
            break;
    }

    free(lineCopy);
    return cmd;
}

int executeCommand(const CommandType* cmd)
{
    if(!cmd)
        return -1;

    for(size_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* h = &registry->handlers[i];
        if(h->type == cmd)
            return h->handler(cmd, NULL);
    }

    return -1;
}


#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>

#include "host/kdTree/print.h"

#include "host/management/interface.h"
#include "host/management/logging.h"

#include "host/environment/init.h"

static CommandRegistry* registry = NULL;

static CommandType getCommandType(char* cmd)
{
    logMessage("Parsing command type", DEBUG);

    if(!cmd)
    {
        logMessage("Null command received", ERROR);
        return UNKNOWN;
    }

    char* token = strtok(cmd, " \t");
    if(!token)
    {
        logMessage("Empty command token", ERROR);
        return UNKNOWN;
    }

    for(size_t i = 0; i < strlen(token); ++i)
        token[i] = tolower(token[i]);

    char msg[128];
    snprintf(msg, sizeof(msg), "Normalized command token: %s", token);
    logMessage(msg, INFO);

    for(size_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* handler = &registry->handlers[i];

        if(handler->longName && strcmp(token, handler->longName) == 0)
        {
            logMessage("Matched long command name", DEBUG);
            return handler->type;
        }

        if(handler->shortName && strcmp(token, handler->shortName) == 0)
        {
            logMessage("Matched short command name", DEBUG);
            return handler->type;
        }
    }

    logMessage("Command not recognized", ERROR);
    return UNKNOWN;
}

static uint8_t countFlags(FlagDefinition* flags)
{
    if(!flags)
        return 0;

    uint8_t count = 0;
    while (flags[count].shortFlag != NULL)
        count++;

    return count;
}

void run()
{
    logMessage("Starting interactive loop", DEBUG);

    if(!registry)
    {
        logMessage("Registry not initialized, initializing", INFO);
        initCommandRegistry();
    }

    char line[256];
    char copy[256];
    bool running = true;

    printHelp();

    while(running)
    {
        printf(" >> ");
        fflush(stdout);

        if(!fgets(line, sizeof(line), stdin))
        {
            logMessage("Input stream closed", ERROR);
            break;
        }

        line[strcspn(line, "\n")] = 0;

        if(strlen(line) == 0)
        {
            logMessage("Empty input line", DEBUG);
            continue;
        }

        strcpy(copy, line);

        char msg[512];
        snprintf(msg, sizeof(msg), "Received input: %s", line);
        logMessage(msg, INFO);

        if(strncmp(line, "help", 4) == 0 || strcmp(line, "h") == 0)
        {
            logMessage("Help command detected", DEBUG);

            char* cmd = strtok(line, " \t");
            cmd = strtok(NULL, " \t");

            if(cmd)
            {
                CommandType type = getCommandType(cmd);

                for(size_t i = 0; i < registry->count; ++i)
                {
                    if(registry->handlers[i].type == type)
                    {
                        printCommandHelp(&registry->handlers[i]);
                        break;
                    }
                }
            }
            else
            {
                logMessage("Printing general help", DEBUG);
                printHelp();;
            }

            continue;
        }

        if(strcmp(line, "quit") == 0 || strcmp(line, "q") == 0)
        {
            logMessage("Termination requested", INFO);
            running = false;
            break;
        }

        CommandType type = getCommandType(line);
        int result = processCommand(type, copy);

        snprintf(msg, sizeof(msg), "Command execution result: %d", result);
        logMessage(msg, INFO);

        if(result != 0)
        {
            logMessage("Command execution failed", ERROR);
            printf("Command execution failed with code %d\n", result);
        }
    }

    logMessage("Shutting down registry", DEBUG);
    freeCommandRegistry();
}

void initCommandRegistry()
{
    logMessage("Initializing command registry", DEBUG);

    registry = (CommandRegistry*)malloc(sizeof(CommandRegistry));
    if(!registry)
        return;

    CommandHandler defaultHandlers[] =
    {
        {BUILD, "build", "b", "Build a new kd-tree", getBuildFlags(), countFlags(getBuildFlags()), &buildParser, handleBuild},

        {INSERT, "insert", "i", "Insert points into tree", getInsertFlags(), countFlags(getInsertFlags()), &insertParser, handleInsert},

        {DELETE, "delete", "d", "Delete points from tree", getDeleteFlags(), countFlags(getDeleteFlags()), &deleteParser, handleDelete},

        {KNN, "knn", "k", "Execute k-nearest neighbor queries", getKnnFlags(), countFlags(getKnnFlags()), &knnParser, handleKNN},

        {RANGE, "range", "r", "Execute orthogonal range query", getRangeFlags(), countFlags(getRangeFlags()), &rangeParser, handleRange},

        {CLUSTER_DPC, "dpc", "dp", "Run Density Peak Clustering", getDpcFlags(), countFlags(getDpcFlags()), &dpcParser, handleClusterDPC},

        {CLUSTER_DBSCAN, "dbscan", "db", "Run DBSCAN clustering", getDbscanFlags(), countFlags(getDbscanFlags()), &dbscanParser, handleClusterDBSCAN},


        {BENCHMARKS, "bench", "be", "Run benchmarks", NULL, 0, &benchmarkParser, handleBenchmark},

        {INFOS, "info", "inf", "Display tree information", getInfoFlags(), countFlags(getInfoFlags()), &infoParser, handleInfo},

        {CONFIG, "config", "c", "Print system parameters", getConfigFlags(), countFlags(getConfigFlags()), &configParser, handleConfig},

        {SET, "set", "s", "Set system parameters", getSetFlags(), countFlags(getSetFlags()), &setParser, handleSet},
    };

    uint8_t numHandlers = sizeof(defaultHandlers) / sizeof(defaultHandlers[0]);
    registry->handlers = (CommandHandler*)malloc(numHandlers * sizeof(CommandHandler));

    if(registry->handlers)
    {
        memcpy(registry->handlers, defaultHandlers, numHandlers * sizeof(CommandHandler));
        registry->count = numHandlers;
    }

    logMessage("Command registry initialized from FlagDefinitions", INFO);
}

void freeCommandRegistry()
{
    logMessage("Releasing command registry", DEBUG);

    if(!registry)
    {
        logMessage("Registry already null", DEBUG);
        return;
    }

    free(registry->handlers);
    free(registry);
    registry = NULL;

    logMessage("Registry released", INFO);
}

void printCommandHelp(CommandHandler* handler)
{
    logMessage("Printing detailed command help", DEBUG);

    if(!handler)
    {
        logMessage("Null handler provided", ERROR);
        return;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Command: %s", handler->longName);
    logMessage(msg, INFO);

    printf("\nCommand: %s", handler->longName);

    if(handler->shortName)
        printf(" (%s)", handler->shortName);

    printf("\n");
    printf("  %s\n", handler->description);
    printf("\n");

    printf("Usage: %s", handler->longName);
    if(handler->flags)
    {
        for(FlagDefinition* f = handler->flags; f->shortFlag != NULL; ++f)
        {
            printf(" [%s", f->shortFlag);
            if(f->argName)
                printf(" %s", f->argName);

            printf("]");
        }
    }

    printf("\n\n");

    if(handler->flags && handler->flags[0].shortFlag != NULL)
    {
        printf("Options:\n");
        for(FlagDefinition* f = handler->flags; f->shortFlag != NULL; ++f)
        {
            printf("  %s", f->shortFlag);
            if(f->longFlag)
                printf(", %s", f->longFlag);

            if(f->argName)
                printf(" %s", f->argName);

            int len = strlen(f->shortFlag) + 2;

            if(f->longFlag)
                len += strlen(f->longFlag) + 3;

            if(f->argName)
                len += strlen(f->argName) + 1;

            int tabs = (32 - len + 7) / 8;
            if(tabs < 1)
                tabs = 1;

            for(int t = 0; t < tabs; t++)
                printf("\t");

            printf("%s", f->description);
            if(f->required)
                printf(" (required)");

            printf("\n");
        }
    }

    printf("\n");
}

void printHelp()
{
    logMessage("Printing global help", DEBUG);

    if(!registry)
    {
        logMessage("Registry not initialized", ERROR);
        return;
    }

    printf("\n======================================================================\n");
    printf("                             PIM KD-TREE                              \n");
    printf("======================================================================\n");
    printf("COMMAND SYNTAX:\n");
    printf("  >> <command> [flags/options]\n\n");

    printf("Available commands:\n");

    printAvailableCommands();

    printf("- help / h \t\t\tShow this help\n");
    printf("- help [command] \t\tShow a detailed help for a command\n");
    printf("- quit / q \t\t\tExit the program\n");
    printf("\nType 'help <command>' for detailed options\n");
}

void printAvailableCommands()
{
    logMessage("Listing available commands", DEBUG);

    if(!registry || !registry->handlers)
    {
        logMessage("Handlers not available", ERROR);
        return;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Total commands: %u", registry->count);
    logMessage(msg, INFO);

    for(uint8_t i = 0; i < registry->count; ++i)
    {
        CommandHandler* h = &registry->handlers[i];
        snprintf(msg, sizeof(msg), "Command available: %s", h->longName);
        logMessage(msg, DEBUG);

        printf("- %s", h->longName);

        if(h->shortName)
            printf(" / %s", h->shortName);

        int len = strlen(h->longName) + 2;
        if(h->shortName)
            len += strlen(h->shortName) + 3;

        int tabs = (30 - len + 7) / 8;
        if(tabs < 1)
            tabs = 1;

        for(int t = 0; t < tabs; ++t)
            printf("\t");

        printf("%s\n", h->description);
    }
}

int processCommand(CommandType type, char* line)
{
    logMessage("Processing command", DEBUG);

    if(!line || strlen(line) == 0)
    {
        logMessage("Invalid input line", ERROR);
        return 1;
    }

    CommandHandler* handler = NULL;

    for(uint8_t i = 0; i < registry->count; ++i)
    {
        if(registry->handlers[i].type == type)
        {
            handler = &registry->handlers[i];
            break;
        }
    }

    if(!handler || !handler->handler)
    {
        logMessage("No valid handler found", ERROR);
        return 1;
    }

    char* argv[32];
    int argc = 0;

    char* token = strtok(line, " \t");
    while(token && argc < 32)
    {
        argv[argc++] = token;
        token = strtok(NULL, " \t");
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Argument count: %d", argc);
    logMessage(msg, INFO);

    if(argc == 0)
    {
        logMessage("No arguments parsed", ERROR);
        return 1;
    }

    void* context = NULL;

    if(handler->parser && handler->parser->parse)
    {
        logMessage("Parsing command arguments", DEBUG);

        context = handler->parser->parse(argv, argc);

        if(!context)
        {
            logMessage("Parsing failed", ERROR);
            return 1;
        }
    }

    logMessage("Executing handler", DEBUG);

    int result = handler->handler(context);

    snprintf(msg, sizeof(msg), "Handler result: %d", result);
    logMessage(msg, INFO);

    if(context && handler->parser && handler->parser->free)
    {
        logMessage("Freeing parser context", DEBUG);
        handler->parser->free(context);
    }

    return result;
}

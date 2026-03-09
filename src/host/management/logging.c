#include <time.h>

#include "management/logging.h"
#include "environment/constants.h"

static Logger logger =
{
    .stream = NULL,
    .level = ERROR
};

static void getCurrentTimestamp(char* buffer, size_t size)
{
    time_t now = time(NULL);
    struct tm* info = localtime(&now);
    strftime(buffer, size, "%H:%M:%S", info);
}

void logError(const char* msg)
{
    if(ERROR > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    fprintf(logger.stream, "%s[%s] ERROR%s: %s%s\n", RED, timestamp, RESET, msg, RESET);
    fflush(logger.stream);
}

void logWarning(const char* msg)
{
    if(WARNING > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    fprintf(logger.stream, "%s[%s] WARNING%s: %s%s\n", YELLOW, timestamp, RESET, msg, RESET);
    fflush(logger.stream);
}

void logDebug(const char* msg)
{
    if(DEBUG > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    fprintf(logger.stream, "%s[%s] DEBUG%s: %s%s\n", BLUE, timestamp, RESET, msg, RESET);
    fflush(logger.stream);
}

void setStream(FILE* stream)
{
    logger.stream = stream ? stream : stdout;
}

void setVerbosity(Severity severity)
{
    logger.level = severity;
}

void logMessage(const char* msg, Severity level)
{
    switch(level)
    {
        case ERROR:
            logError(msg);
            break;
        case WARNING:
            logWarning(msg);
            break;
        case DEBUG:
            logDebug(msg);
            break;
    }
}

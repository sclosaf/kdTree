#include <unistd.h>
#include <time.h>

#include "host/management/logging.h"
#include "host/environment/constants.h"

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

static void logError(const char* msg)
{
    if(ERROR > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    if(isatty(fileno(logger.stream)))
        fprintf(logger.stream, "%s[%s] ERROR%s: %s%s\n", ANSI_RED, timestamp, ANSI_RESET, msg, ANSI_RESET);
    else
        fprintf(logger.stream, "[%s] ERROR: %s\n", timestamp, msg);

    fflush(logger.stream);
}

static void logDebug(const char* msg)
{
    if(DEBUG > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    if(isatty(fileno(logger.stream)))
        fprintf(logger.stream, "%s[%s] DEBUG%s: %s%s\n", ANSI_YELLOW, timestamp, ANSI_RESET, msg, ANSI_RESET);
    else
        fprintf(logger.stream, "[%s] DEBUG: %s\n", timestamp, msg);

    fflush(logger.stream);
}

static void logInfo(const char* msg)
{
    if(INFO > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    if(isatty(fileno(logger.stream)))
        fprintf(logger.stream, "%s[%s] INFO%s: %s%s\n", ANSI_BLUE, timestamp, ANSI_RESET, msg, ANSI_RESET);
    else
        fprintf(logger.stream, "[%s] INFO: %s\n", timestamp, msg);

    fflush(logger.stream);
}

static void logTest(const char* msg)
{
    if(TEST > logger.level || logger.stream == NULL)
        return;

    char timestamp[20];
    getCurrentTimestamp(timestamp, sizeof(timestamp));

    if(isatty(fileno(logger.stream)))
        fprintf(logger.stream, "%s[%s] TEST%s: %s%s\n", ANSI_GREEN, timestamp, ANSI_RESET, msg, ANSI_RESET);
    else
        fprintf(logger.stream, "[%s] TEST: %s\n", timestamp, msg);

    fflush(logger.stream);
}


void setStream(FILE* stream)
{
    logger.stream = stream ? stream : stdout;
}

FILE* getStream()
{
    return logger.stream;
}

void setVerbosity(Severity severity)
{
    logger.level = severity;
}

Severity getVerbosity()
{
    return logger.level;
}

void logMessage(const char* msg, Severity level)
{
    switch(level)
    {
        case ERROR:
            logError(msg);
            break;
        case DEBUG:
            logDebug(msg);
            break;
        case INFO:
            logInfo(msg);
            break;
        case TEST:
            logTest(msg);
            break;
        default:
            break;
    }
}

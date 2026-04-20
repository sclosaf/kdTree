#ifndef MANAGEMENT_LOGGING_H
#define MANAGEMENT_LOGGING_H

#include <stdio.h>
#include <stdint.h>

typedef enum Severity
{
    NONE,
    TEST,
    ERROR,
    DEBUG,
    INFO
} Severity;

typedef struct Logger
{
    FILE* stream;
    Severity level;
} Logger;

void setStream(FILE* stream);
FILE* getStream();

void setVerbosity(Severity severity);
Severity getVerbosity();

void logMessage(const char* msg, Severity level);

#endif

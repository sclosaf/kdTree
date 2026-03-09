#ifndef MANAGEMENT_LOGGING_H
#define MANAGEMENT_LOGGING_H

#include <stdio.h>
#include <stdint.h>

typedef enum Severity
{
    ERROR,
    WARNING,
    DEBUG
} Severity;

typedef struct Logger
{
    FILE* stream;
    Severity level;
} Logger;

void setStream(FILE* stream);
void setVerbosity(Severity severity);

void logMessage(const char* msg, Severity level);

#endif

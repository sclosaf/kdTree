#ifndef MANAGEMENT_LOGGING_H
#define MANAGEMENT_LOGGING_H

#define RED     "\x1b[31m"
#define YELLOW  "\x1b[33m"
#define BLUE    "\x1b[34m"

#define RESET   "\x1b[0m"

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

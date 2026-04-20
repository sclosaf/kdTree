#include <string.h>

#include "host/management/timer.h"
#include "host/management/logging.h"

void timerStart(Performance* timer)
{
    logMessage("Starting timer", DEBUG);

    if(!timer)
    {
        logMessage("Null timer provided", ERROR);
        return;
    }

    memset(timer, 0, sizeof(Performance));
    clock_gettime(CLOCK_MONOTONIC, &timer->startTime);

    logMessage("Timer initialized and started", INFO);
}

void timerStop(Performance* timer)
{
    logMessage("Stopping timer", DEBUG);

    if(!timer)
    {
        logMessage("Null timer provided", ERROR);
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &timer->endTime);

    timer->elapsedMS = (timer->endTime.tv_sec - timer->startTime.tv_sec) * 1000.0 + (timer->endTime.tv_nsec - timer->startTime.tv_nsec) / 1e6;

    char msg[128];
    snprintf(msg, sizeof(msg), "Elapsed time: %.3f ms", timer->elapsedMS);
    logMessage(msg, INFO);
}

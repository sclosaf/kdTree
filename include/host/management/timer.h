#ifndef MANAGEMENT_TIMER_H
#define MANAGEMENT_TIMER_H

#include <stdint.h>
#include <time.h>
#include <stdio.h>

typedef struct Performance
{
    struct timespec startTime;
    struct timespec endTime;
    double elapsedMS;
} Performance;

void timerStart(Performance* timer);
void timerStop(Performance* timer);

#define TIME_OP(name, ...) \
    do { \
        Performance timer; \
        timerStart(&timer); \
        __VA_ARGS__; \
        timerStop(&timer); \
        printf("[%s] Elapsed time: %.3f ms\n", name, timer.elapsedMS); \
    } while(0)

#endif

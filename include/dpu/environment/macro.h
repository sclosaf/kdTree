#ifndef DPU_ENVIRONMENT_MACRO_H
#define DPU_ENVIRONMENT_MACRO_H

#include <defs.h>

#define DPU_LOG(fmt, ...) do { \
    if (me() == 0) { \
        printf("[DPU] " fmt "\n", ##__VA_ARGS__); \
    } \
} while (0)

#endif

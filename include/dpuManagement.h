#ifndef DPUMANAGEMENT_H
#define DPUMANAGEMENT_H

#include <dpu.h>
#include "types.h"

typedef struct
{
    struct DSet set;
    u32 nDpus;
    u32 nRanks;
    u32 currentDpuId;
} DPUContext;

typedef struct
{
    u32 totalPoints;
    u32 pointsPerDpu;
    u32 dim;
    f32 alpha;
    f32 beta;
    u64 baseAddress;
} DPUKernelArgs;

DPUContext* dpuInitContext(u32 nDpus);
void dpuCleanupContext(DPUContext* ctx);

int dpuLaunchKernel(DPUContext* ctx, const char* program_path, DPUKernelArgs* args);
int dpuLaunchSpecificDpu(DPUContext* ctx, u32 dpuId, const char* path, DPUKernelArgs* args);
int dpuLaunchAllDpus(DPUContext* ctx, const char* path, DPUKernelArgs* args);

int dpuTransferDataToDpu(DPUContext* ctx, u32 dpuId, const void* data, size_t size, DpuXferFlags flags);
int dpuTransferDataFromDpu(DPUContext* ctx, u32 dpuId, void* data, size_t size, DpuXferFlags flags);

#endif

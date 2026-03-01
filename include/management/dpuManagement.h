#ifndef DPUMANAGEMENT_H
#define DPUMANAGEMENT_H

#include <dpu.h>

typedef struct
{
    struct dpu_set_t set;
    uint32_t nDpus;
    uint32_t nRanks;
    uint32_t currentDpuId;
} DPUContext;

typedef struct
{
    uint32_t totalPoints;
    uint32_t pointsPerDpu;
    uint32_t dim;
} DPUKernelArgs;

DPUContext* dpuInitContext(uint32_t nDpus);
void dpuCleanupContext(DPUContext* ctx);

int dpuLaunchSpecificDpu(DPUContext* ctx, uint32_t dpuId, const char* path, DPUKernelArgs* args);
int dpuLaunchAllDpus(DPUContext* ctx, const char* path, DPUKernelArgs* args);

int dpuTransferDataToDpu(DPUContext* ctx, uint32_t dpuId, void* data, size_t size, dpu_xfer_flags_t flags);
int dpuTransferDataFromDpu(DPUContext* ctx, uint32_t dpuId, void* data, size_t size, dpu_xfer_flags_t flags);

int dpuBroadcastFromAllDpus(DPUContext* ctx, const char* symbolName, void* data, size_t size, dpu_xfer_flags_t flags);
int dpuSyncAllDpus(DPUContext* ctx);

#endif

#include "dpuManagement.h"

DPUContext* dpuInitContext(u32 nDpus)
{
    DPUContext* ctx = (DPUContext*)malloc(sizeof(DPUContext));
    if(!ctx)
        return NULL;

    DPU_ASSERT(dpu_alloc(nDpus, NULL, &ctx->set));
    DPU_ASSERT(dpu_get_nr_dpus(ctx->set, &ctx->nDpus));

    DPU_ASSERT(dpu_get_nr_ranks(ctx->set, &ctx->nRanks));
    ctx->currentDpuId = 0;

    return ctx;
}

void dpuCleanupContext(DPUContext* ctx)
{
    if(ctx)
    {
        DPU_ASSERT(dpu_free(ctx->set));
        free(ctx);
    }
}

int dpuLaunchSpecificDpu(DPUContext* ctx, u32 dpuId, const char* path, DPUKernelArgs* args)
{
    if(!ctx || dpuId >= ctx->nDpus)
        return -1;

    struct dpu_set_t dpu;
    u32 currentId = 0;
    bool found = 0;

    DPU_FOREACH(ctx->set, dpu)
    {
        if(currentId == dpuId)
        {
            found = 1;
            break;
        }
        currentId++;
    }

    if(!found)
        return -1;

    DPU_ASSERT(dpu_load(dpu, path, NULL));

    if(args)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, args));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "args", 0, sizeof(DPUKernelArgs), DPU_XFER_DEFAULT));
    }

    DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));

    return 0;
}


int dpuLaunchAllDpus(DPUContext* ctx, const char* path, DPUKernelArgs* args)
{
    if(!ctx)
        return -1;

    DPU_ASSERT(dpu_load(ctx->set, path, NULL));

    if(args)
        DPU_ASSERT(dpu_broadcast_to(ctx->set, "args", 0, args, sizeof(DPUKernelArgs), DPU_XFER_DEFAULT));

    DPU_ASSERT(dpu_launch(ctx->set, DPU_SYNCHRONOUS));

    return 0;
}

int dpuTransferDataToDpu(DPUContext* ctx, u32 dpuId, const void* data, size_t size, dpu_xfer_flags_t flags)
{
    if(!ctx || dpuId >= ctx->nDpus)
        return -1;

    struct DSet dpu;
    u32 currentId = 0;

    DPU_FOREACH(ctx->set, dpu)
    {
        if(currentId == dpuId)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, data));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "input", 0, size, flags));
            return 0;
        }
        currentId++;
    }

    return -1;
}

int dpuTransferDataFromDpu(DPUContext* ctx, u32 dpuId, void* data, size_t size, dpu_xfer_flags_t flags)
{
    if(!ctx || dpuId>= ctx->nDpus)
        return -1;

    struct dSet dpu;
    u32 currentId= 0;

    DPU_FOREACH(ctx->set, dpu)
    {
        if(currentId == dpuId)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, data));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "output", 0, size, flags));
            return 0;
        }
        currentId++;
    }

    return -1;
}

#include "management.h"

DPUContext* dpuInitContext(u32 nDpus)
{
    DPUContext* ctx = (DPUContext*)malloc(sizeof(DPUContext));
    if(!ctx)
        return NULL;

    DPU_ASSERT(dpu_alloc(nDpus, NULL, &ctx->set));
    DPU_ASSERT(dpu_get_nr_dpus(ctx->set, &ctx->nDpus));

    DPU_ASSERT(dpu_get_nr_ranks(ctx->dpu_set, &ctx->nRanks));
    ctx->current_dpu_id = 0;

    printf("DPU Context initialized: %u DPUs across %u ranks\n", ctx->nDpus, ctx->nRanks);
    return ctx;
}

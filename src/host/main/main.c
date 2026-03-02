#include <stdio.h>
#include <dpu.h>
#include <dpu_types.h>

int main()
{
    struct dpu_set_t dpu_set, rank;
    uint32_t nr_dpus, nr_ranks;
    uint32_t rank_id = 0, dpu_id = 0;

    DPU_ASSERT(dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpu_set));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_dpus));
    DPU_ASSERT(dpu_get_nr_ranks(dpu_set, &nr_ranks));

    printf("Total DPUs: %u\n", nr_dpus);
    printf("Total ranks: %u\n", nr_ranks);

    DPU_ASSERT(dpu_free(dpu_set));

    return 0;
}

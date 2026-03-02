#include <stdio.h>
#include <dpu.h>

int main()
{
    struct dpu_set_t all_dpus;
    uint32_t nr_dpus;

    DPU_ASSERT(dpu_probe(NULL, &all_dpus, &nr_dpus));

    printf("Total number of DPUs available: %u\n", nr_dpus);

    struct dpu_set_t chip_dpus;
    DPU_FOREACH(dpu, all_dpus) {
        struct dpu_t *dpu_ptr;
        dpu_ptr = &dpu;
        printf("Chip ID: 0x%x\n", dpu_ptr->chip_id);
        printf("Rank number: %u\n", dpu_ptr->rank->nr_of_dpus);
    }

    dpu_free(all_dpus);
    return 0;
}

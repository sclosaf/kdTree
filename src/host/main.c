#include <stdio.h>
#include <dpu.h>

#ifndef DPU_BINARY
#define DPU_BINARY "build/dpu_main.dpu"
#endif

int main()
{
    struct dpu_set_t dpu_set;
    uint32_t nr_dpus;

    printf("Starting test\n");

    DPU_ASSERT(dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpu_set));
    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_dpus));
    printf("Allocated %d DPUs\n", nr_dpus);

    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));
    printf("Program loaded\n");

    DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    printf("Launch done\n");

    dpu_free(dpu_set);
    printf("Test completed\n");

    return 0;
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>


#include "management/specifics.h"

void printDpuInfo(struct dpu_set_t dpu, u32 dpu_id, u32 rank_id)
{
    struct dpu_info_t info;

    printf("\n--- DPU %u (Rank %u) ---\n", dpu_id, rank_id);

    DPU_ASSERT(dpu_get_info(dpu, &info));

    printf("Hardware Info:\n");
    printf("  - Name: %s\n", info.name);
    printf("  - HW Revision: %u\n", info.hw_revision);
    printf("  - Threads per DPU: %u\n", info.num_of_threads);
    printf("  - MRAM size: %llu bytes (%.2f MB)\n", info.mem_size, info.mem_size / (1024.0 * 1024.0));
    printf("  - WRAM size: %u bytes\n", info.wram_size);
    printf("  - IRAM size: %u bytes\n", info.iram_size);

    printf("  - Clock frequency: %u MHz\n", info.clock_frequency);
    printf("  - Timing: setup=%u, latency=%u, hold=%u\n", info.timing_setup, info.timing_access, info.timing_hold);

    dpu_state_t state;
    DPU_ASSERT(dpu_get_state(dpu, &state));
    printf("  - Current state: %d\n", state);

    dpu_fault_t fault;
    DPU_ASSERT(dpu_get_fault(dpu, &fault));
    printf("  - Fault status: %d\n", fault);

    dpu_slice_id_t slice_id;
    dpu_member_id_t member_id;
    DPU_ASSERT(dpu_get_slice_id(dpu, &slice_id));
    DPU_ASSERT(dpu_get_member_id(dpu, &member_id));
    printf("  - Slice ID: %u\n", slice_id);
    printf("  - Member ID: %u\n", member_id);
}

void printRankInfo(struct dpu_set_t rank, u32 rank_id)
{
    printf("\n--- Rank %u Information ---\n", rank_id);

    uint32_t nr_dpus_per_rank;
    DPU_ASSERT(dpu_get_nr_dpus_per_rank(rank, &nr_dpus_per_rank));
    printf("  DPUs per rank: %u\n", nr_dpus_per_rank);
}

void printInfoSystem()
{
    struct dpu_set_t dpu_set, dpu, rank;
    u32 nr_dpus, nr_ranks, dpu_id = 0, rank_id = 0;


    DPU_ASSERT(dpu_get_nr_dpus(&nr_dpus));
    printf("Total DPUs available in system: %u\n", nr_dpus);

    DPU_ASSERT(dpu_get_nr_ranks(&nr_ranks));
    printf("Total ranks available in system: %u\n", nr_ranks);


    printf("Allocating DPU set...\n");
    DPU_ASSERT(dpu_alloc(nr_dpus, NULL, &dpu_set));


    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_dpus));
    DPU_ASSERT(dpu_get_nr_ranks(dpu_set, &nr_ranks));
    printf("Successfully allocated:\n");
    printf("  - %u DPUs\n", nr_dpus);
    printf("  - %u ranks\n", nr_ranks);




    struct dpu_set_info_t set_info;
    DPU_ASSERT(dpu_get_set_info(dpu_set, &set_info));
    printf("Set Info:\n");
    printf("  - Total DPUs: %u\n", set_info.nr_dpus);
    printf("  - Total ranks: %u\n", set_info.nr_ranks);
    printf("  - DPUs per rank: %u\n", set_info.nr_dpus_per_rank);
    printf("  - Alloc mode: %u\n", set_info.mode);



    DPU_RANK_FOREACH(dpu_set, rank)
    {
        print_rank_info(rank, rank_id);
        rank_id++;
    }




    u32 sample_size = (nr_dpus < 3) ? nr_dpus : 3;
    printf("Showing details for %u sample DPUs:\n", sample_size);

    dpu_id = 0;
    rank_id = 0;
    u32 current_rank = 0;

    DPU_FOREACH(dpu_set, dpu) {
        DPU_ASSERT(dpu_get_rank_id(dpu, &current_rank));

        if(dpu_id < sample_size) {
            print_dpu_info(dpu, dpu_id, current_rank);
        }
        dpu_id++;
    }


    struct dpu_set_t first_dpu = dpu_get_dpu_by_id(dpu_set, 0);
    struct dpu_info_t info;
    DPU_ASSERT(dpu_get_info(first_dpu, &info));

    printf("Memory Map for DPU:\n");
    printf("  MRAM: 0x00000000 - 0x%08llx (%llu bytes)\n", info.mem_size - 1, info.mem_size);
    printf("    - Kernel area (read-only)\n");
    printf("    - Heap area: 0x%08x - 0x%08llx\n", DPU_MRAM_HEAP_POINTER_OFFSET, info.mem_size - 1);

    printf("\n  WRAM (per thread): 0x0000 - 0x%04x (%u bytes)\n", info.wram_size - 1, info.wram_size);
    printf("    - Stack\n    - Heap\n    - Static data\n");

    printf("\n  IRAM: 0x0000 - 0x%04x (%u bytes)\n", info.iram_size - 1, info.iram_size);
    printf("    - Instructions\n");


    printf("Estimated performance per DPU:\n");
    printf("  - Peak integer ops: ~%u MIPS\n", info.clock_frequency);
    printf("  - MRAM bandwidth: ~%.1f GB/s\n",info.clock_frequency * 64.0 / 1000.0);
    printf("  - WRAM access: 1 cycle\n");

    DPU_ASSERT(dpu_free(dpu_set));
}

#include <dpu.h>
#include <stdint.h>
#include <sys/sysinfo.h>
#include <stdio.h>

#include "management/metrics.h"

unsigned long long getTotalRam()
{
    struct sysinfo info;
    if(sysinfo(&info) != 0)
        return 0;

    return (unsigned long long)info.totalram * info.mem_unit;
}

unsigned long long getAvailableRam()
{
    FILE *file = fopen("/proc/meminfo", "r");
    if(!file)
        return 0;

    char line[256];
    unsigned long long memAvailable = 0;

    while(fgets(line, sizeof(line), file))
        if(sscanf(line, "MemAvailable: %llu kB", &memAvailable) == 1)
            break;

    fclose(file);
    return memAvailable * 1024;
}

uint32_t getNumDPUs()
{
    struct dpu_set_t dpuSet;
    uint32_t nDpus = 0;

    if (dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpuSet) != DPU_OK)
        return 0;

    if (dpu_get_nr_dpus(dpuSet, &nDpus) != DPU_OK)
        nDpus = 0;

    dpu_free(dpuSet);
    return nDpus;
}

uint32_t getNumRanks()
{
    struct dpu_set_t dpuSet;
    uint32_t nRanks = 0;

    if (dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpuSet) != DPU_OK)
        return 0;

    if (dpu_get_nr_ranks(dpuSet, &nRanks) != DPU_OK)
        nRanks = 0;

    dpu_free(dpuSet);
    return nRanks;
}

void printSystemMetrics()
{
    unsigned long long totalRam = getTotalRam();
    unsigned long long availableRam = getAvailableRam();

    printf("=== CPU / Host ===\n");
    printf("Total RAM       : %llu bytes (%.2f GB)\n", totalRam, (double)totalRam / (1024.0 * 1024.0 * 1024.0));
    printf("Available RAM   : %llu bytes (%.2f GB)\n",
           availableRam, (double)availableRam / (1024.0 * 1024.0 * 1024.0));

    uint32_t nDpus = getNumDPUs();
    uint32_t nRanks = getNumRanks();

    printf("\n=== DPU / UPMEM ===\n");
    printf("Total DPUs      : %u\n", nDpus);
    printf("Total ranks     : %u\n", nRanks);
}

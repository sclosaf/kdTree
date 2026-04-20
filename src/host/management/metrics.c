#include <dpu.h>
#include <stdint.h>
#include <sys/sysinfo.h>
#include <stdio.h>

#include "host/management/metrics.h"
#include "host/management/logging.h"

unsigned long long getTotalRam()
{
    logMessage("Retrieving total system RAM", DEBUG);

    struct sysinfo info;
    if(sysinfo(&info) != 0)
    {
        logMessage("sysinfo call failed", ERROR);
        return 0;
    }

    unsigned long long total = (unsigned long long)info.totalram * info.mem_unit;

    char msg[128];
    snprintf(msg, sizeof(msg), "Total RAM: %llu bytes", total);
    logMessage(msg, INFO);

    return total;
}

unsigned long long getAvailableRam()
{
    logMessage("Retrieving available system RAM", DEBUG);

    FILE *file = fopen("/proc/meminfo", "r");
    if(!file)
    {
        logMessage("Failed to open /proc/meminfo", ERROR);
        return 0;
    }

    char line[256];
    unsigned long long memAvailable = 0;

    while(fgets(line, sizeof(line), file))
    {
        if(sscanf(line, "MemAvailable: %llu kB", &memAvailable) == 1)
        {
            logMessage("MemAvailable entry found", DEBUG);
            break;
        }
    }

    fclose(file);

    unsigned long long available = memAvailable * 1024;

    char msg[128];
    snprintf(msg, sizeof(msg), "Available RAM: %llu bytes", available);
    logMessage(msg, INFO);

    return available;
}

uint32_t getNumDPUs()
{
    logMessage("Querying number of DPUs", DEBUG);

    struct dpu_set_t dpuSet;
    uint32_t nDpus = 0;

    if(dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpuSet) != DPU_OK)
    {
        logMessage("DPU allocation failed", ERROR);
        return 0;
    }

    if(dpu_get_nr_dpus(dpuSet, &nDpus) != DPU_OK)
    {
        logMessage("Failed to retrieve DPU count", ERROR);
        nDpus = 0;
    }

    dpu_free(dpuSet);

    char msg[128];
    snprintf(msg, sizeof(msg), "Number of DPUs: %u", nDpus);
    logMessage(msg, INFO);

    return nDpus;
}

uint32_t getNumRanks()
{
    logMessage("Querying number of ranks", DEBUG);

    struct dpu_set_t dpuSet;
    uint32_t nRanks = 0;

    if(dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpuSet) != DPU_OK)
    {
        logMessage("DPU allocation failed", ERROR);
        return 0;
    }

    if(dpu_get_nr_ranks(dpuSet, &nRanks) != DPU_OK)
    {
        logMessage("Failed to retrieve rank count", ERROR);
        nRanks = 0;
    }

    dpu_free(dpuSet);

    char msg[128];
    snprintf(msg, sizeof(msg), "Number of ranks: %u", nRanks);
    logMessage(msg, INFO);

    return nRanks;
}

void printSystemMetrics()
{
    logMessage("Printing system metrics", DEBUG);

    unsigned long long totalRam = getTotalRam();
    unsigned long long availableRam = getAvailableRam();

    if(totalRam == 0)
        logMessage("Total RAM unavailable", ERROR);

    if(availableRam == 0)
        logMessage("Available RAM unavailable", ERROR);

    printf("=== CPU / Host ===\n");
    printf("Total RAM       : %llu bytes (%.2f GB)\n", totalRam, (double)totalRam / (1024.0 * 1024.0 * 1024.0));
    printf("Available RAM   : %llu bytes (%.2f GB)\n", availableRam, (double)availableRam / (1024.0 * 1024.0 * 1024.0));

    uint32_t nDpus = getNumDPUs();
    uint32_t nRanks = getNumRanks();

    if(nDpus == 0)
        logMessage("DPU count unavailable or zero", ERROR);

    if(nRanks == 0)
        logMessage("Rank count unavailable or zero", ERROR);

    printf("\n=== DPU / UPMEM ===\n");
    printf("Total DPUs      : %u\n", nDpus);
    printf("Total ranks     : %u\n", nRanks);

    logMessage("System metrics printed", INFO);
}

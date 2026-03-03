#ifndef MANAGEMENT_SPECIFICS_H
#define MANAGEMENT_SPECIFICS_H

unsigned long long getTotalRam();
unsigned long long getAvailableRam();
uint32_t getNumDPUs();
uint32_t getNumRanks();
uint32_t getWramAvailable();

void printSystemMetrics();

#endif

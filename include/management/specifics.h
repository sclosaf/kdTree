#ifndef SPECIFICS_H
#define SPECIFICS_H

#include <dpu.h>

#include "utils/types.h"

void printInfoSystem()
void printRankInfo(struct dpu_set_t rank, u32 rank_id)
void printDpuInfo(struct dpu_set_t dpu, u32 dpu_id, u32 rank_id)

#endif

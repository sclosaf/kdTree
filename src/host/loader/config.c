#include <dpu.h>
#include <dpu.h>

#include "dpu/environment/config.h"
#include "host/environment/init.h"

#include "host/management/logging.h"

void sendConfigToDpu(struct dpu_set_t set)
{
    logMessage("Preparing configuration broadcast to all DPUs", DEBUG);

    DpuConfig dpuConfig = {
        .nPoint = getNPoint(),
        .nPim = getNPim(),
        .dimensions = getDimensions(),
        .alpha = getAlpha(),
        .beta = getBeta(),
        .minCoord = getMinCoord(),
        .maxCoord = getMaxCoord(),
        .leafWrapThreshold = getLeafWrapThreshold(),
        .oversamplingRate = getOversamplingRate(),
        .sketchHeight = getSketchHeight(),
        .chunkSize = getChunkSize()
    };

    logMessage("DpuConfig structure populated for broadcast", DEBUG);

    DPU_ASSERT(dpu_broadcast_to(set, "dpuConfig", 0, &dpuConfig, sizeof(DpuConfig), DPU_XFER_DEFAULT));
    logMessage("DpuConfig structure sent successfully", INFO);
}

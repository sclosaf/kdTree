#include <stdlib.h>

#include "kdTree/print.h"

#include "environment/init.h"

#include "management/metrics.h"
#include "management/interface.h"

int handleBuild(void* context)
{
    BuildContext ctx = *(BuildContext*)context;
    free(context);

    switch(ctx)
    {
        case CHIP:
            if(getData()->tree)
                freeData();

            getData()->tree = buildOnChip(readDataset(), getConfig()->nPoints);

            if(getData()->tree)
                return 0;
            else
                return -1;

        case PIM:
             if(getData()->tree)
                freeData();

            getData()->tree = buildPIMkdtree(readDataset(), getConfig()->nPoints);

            if(getData()->tree)
                return 0;
            else
                return -1;

        default:
            return -1;
    }

    return 0;
}

int handleInsert(void* context)
{
    return 0;
}

int handleDelete(void* context)
{
    return 0;
}

int handleKNN(void* context)
{
    return 0;
}

int handleRange(void* context)
{
    return 0;
}

int handleClusterDPC(void* context)
{
    return 0;
}

int handleClusterDBSCAN(void* context)
{
    return 0;
}

int handleTest(void* context)
{
    return 0;
}

int handleBenchmark(void* context)
{
    return 0;
}

int handleInfo(void* context)
{
    PrintConfig* ctx = (PrintConfig*)context;
    free(context);

    printKDTree(getData()->tree, ctx);

    return 0;
}

int handleConfig(void* context)
{
    ConfigContext ctx = *(ConfigContext*)context;
    free(context);

    switch(ctx)
    {
        case INIT:
            initConfig();
            return 0;

        case RESET:
            resetConfig();
            return 0;

        case CONFIG:
            printConfig();
            return 0;

        case SPECIFICS:
            printSystemMetrics();
            return 0;

        case DATASET:
            printDataset(readDataset(), getConfig()->nPoints);
            return 0;

        default:
            return -1;
    }
}

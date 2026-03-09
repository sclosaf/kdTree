#include "kdTree/print.h"

#include "environment/init.h"

#include "management/metrics.h"
#include "management/interface.h"

int handleBuild(void* context)
{
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
    Style ctx = *(Style*)context;
    free(context);

    switch(ctx)
    {
        case COMPACT:
            break;

        case TREE:
            break;

        case DETAILED:
            break;
    }

    return 0;
}

int handleValidate(void* context)
{
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

        case PRINT:
            printConfig();
            return 0;

        case SPECIFICS:
            printSystemMetrics();
            return 0;

        default:
            return -1;
    }
}

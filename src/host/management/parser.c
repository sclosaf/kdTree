#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>

#include "host/kdTree/print.h"

#include "host/management/logging.h"
#include "host/management/interface.h"
#include "host/environment/init.h"

static Dataset stringToDataset(const char* str)
{
    logMessage("Converting string to dataset", DEBUG);

    if(!str)
    {
        logMessage("Null dataset string", ERROR);
        return DEFAULT100;
    }

    if(strcmp(str, "d100") == 0)
        return DEFAULT100;
    else if(strcmp(str, "d1000") == 0)
        return DEFAULT1000;
    else if(strcmp(str, "d1mln") == 0)
        return DEFAULT1MLN;
    else if(strcmp(str, "d10mln") == 0)
        return DEFAULT10MLN;
    else if(strcmp(str, "benchmark") == 0)
        return BENCHMARK;

    logMessage("Unknown dataset, using default", INFO);
    return DEFAULT100;
}

bool validatePositiveInt(char** args, uint8_t argCount)
{
    logMessage("Validating positive integer", DEBUG);

    if(argCount != 1)
    {
        logMessage("Invalid argument count", ERROR);
        return false;
    }

    char* endptr;
    long val = strtol(args[0], &endptr, 10);

    bool valid = (*endptr == '\0' && val > 0);

    logMessage(valid ? "Valid positive integer" : "Invalid positive integer", INFO);
    return valid;
}

bool validateNonNegativeInt(char** args, uint8_t argCount)
{
    logMessage("Validating non-negative integer", DEBUG);

    if(argCount != 1)
    {
        logMessage("Invalid argument count", ERROR);
        return false;
    }

    char* endptr;
    long val = strtol(args[0], &endptr, 10);

    bool valid = (*endptr == '\0' && val >= 0);

    logMessage(valid ? "Valid non-negative integer" : "Invalid non-negative integer", INFO);
    return valid;
}

bool validateFloat(char** args, uint8_t argCount)
{
    logMessage("Validating float", DEBUG);

    if(argCount != 1)
    {
        logMessage("Invalid argument count", ERROR);
        return false;
    }

    char* endptr;
    strtod(args[0], &endptr);

    bool valid = (*endptr == '\0');

    logMessage(valid ? "Valid float" : "Invalid float", INFO);
    return valid;
}

bool validateCoordinates(char** args, uint8_t argCount)
{
    logMessage("Validating coordinates", DEBUG);

    if(argCount == 0 || argCount % getDimensions() != 0)
    {
        logMessage("Invalid coordinate count", ERROR);
        return false;
    }

    for(uint8_t i = 0; i < argCount; ++i)
    {
        char* endptr;
        strtod(args[i], &endptr);
        if(*endptr != '\0')
        {
            logMessage("Invalid coordinate value", ERROR);
            return false;
        }
    }

    logMessage("Coordinates valid", INFO);
    return true;
}

bool validateRangeCoordinates(char** args, uint8_t argCount)
{
    logMessage("Validating range coordinates", DEBUG);

    uint8_t dims = getDimensions();

    if(argCount == 0 || argCount % (2 * dims) != 0)
    {
        logMessage("Invalid range coordinate count", ERROR);
        return false;
    }

    for(uint8_t i = 0; i < argCount; ++i)
    {
        char* endptr;
        strtod(args[i], &endptr);
        if(*endptr != '\0')
        {
            logMessage("Invalid coordinate value", ERROR);
            return false;
        }
    }

    logMessage("Range coordinates valid", INFO);
    return true;
}

bool validateDataset(char** args, uint8_t argCount)
{
    logMessage("Validating dataset", DEBUG);

    if(argCount != 1)
    {
        logMessage("Invalid argument count", ERROR);
        return false;
    }

    const char* validDatasets[] = {"d100", "d1000", "d1mln", "d10mln", "benchmark"};

    for(size_t i = 0; i < sizeof(validDatasets)/sizeof(validDatasets[0]); ++i)
    {
        if(strcmp(args[0], validDatasets[i]) == 0)
        {
            logMessage("Dataset valid", INFO);
            return true;
        }
    }

    logMessage("Dataset invalid", ERROR);
    return false;
}

static FlagDefinition buildFlags[] =
{
    {"-c", "--chip", "Build on CHIP", NULL, 0, false, NULL},
    {"-p", "--pim", "Build on PIM", NULL, 0, false, NULL},
    {"-d", "--dataset", "Dataset to use", "<dataset>", 1, false, validateDataset},
    FLAG_TERMINATOR
};

static FlagDefinition insertFlags[] =
{
    {"-f", "--file", "Insert from file", "[count]", 0, false, NULL},
    {"-r", "--random", "Insert random points", "<count>", 1, false, validatePositiveInt},
    {"-c", "--coordinates", "Insert specific coordinates", "<x1 y1 ...>", 0, false, validateCoordinates},
    {"-o", "--offset", "Offset multiplier", "<multiplier>", 1, false, validateNonNegativeInt},
    {"-d", "--dataset", "Dataset to use", "<dataset>", 1, false, validateDataset},
    FLAG_TERMINATOR
};

static FlagDefinition deleteFlags[] =
{
    {"-c", "--coordinates", "Delete by coordinates", "<x1 y1 ...>", 0, false, validateCoordinates},
    {"-r", "--random", "Delete random points", "<count>", 1, false, validatePositiveInt},
    {"-l", "--leaf", "Delete leaf nodes", "<count>", 1, false, validatePositiveInt},
    FLAG_TERMINATOR
};

static FlagDefinition knnFlags[] =
{
    {"-k", "--k", "Number of neighbors", "<k>", 1, true, validatePositiveInt},
    {"-a", "--approximate", "Use approximate KNN with epsilon", "<epsilon>", 1, false, validateFloat},
    {"-c", "--coordinates", "Specific query points", "<x1 y1 ...>", 0, false, validateCoordinates},
    {"-r", "--random", "Generate random queries", "<count>", 1, false, validatePositiveInt},
    FLAG_TERMINATOR
};

static FlagDefinition rangeFlags[] =
{
    {"-c", "--coordinates", "Range query with coordinates (min1 max1 min2 max2 ...)", "<min1 max1 min2 max2 ...>", 0, false, validateRangeCoordinates},
    {"-r", "--random", "Generate random range queries", "<count>", 1, false, validatePositiveInt},
    FLAG_TERMINATOR
};

static FlagDefinition dpcFlags[] =
{
    {"-k", "--knn", "Number of neighbors for density estimation", "<k>", 1, false, validatePositiveInt},
    {"-d", "--density", "Minimum density threshold (0-1)", "<threshold>", 1, false, validateFloat},
    {"-D", "--delta", "Minimum delta threshold (0-1)", "<threshold>", 1, false, validateFloat},
    FLAG_TERMINATOR
};

static FlagDefinition dbscanFlags[] =
{
    {"-e", "--epsilon", "Epsilon radius for neighborhood search", "<epsilon>", 1, true, validateFloat},
    {"-m", "--minpts", "Minimum points to form a cluster", "<minPts>", 1, true, validatePositiveInt},
    {"-g", "--gridsize", "Grid cell size (optional, defaults to epsilon)", "<size>", 1, false, validateFloat},
    FLAG_TERMINATOR
};

static FlagDefinition infoFlags[] =
{
    {"-c", "--compact", "Compact view", NULL, 0, false, NULL},
    {"-t", "--tree", "Tree view", NULL, 0, false, NULL},
    {"-d", "--detailed", "Detailed view", NULL, 0, false, NULL},
    {"-v", "--validate", "Validate tree", NULL, 0, false, NULL},
    {"-a", "--approximate", "Approximate view", NULL, 0, false, NULL},
    {"-r", "--replicas", "Show replicas", NULL, 0, false, NULL},
    {"-s", "--stats", "Statistics", NULL, 0, false, NULL},
    {"-m", "--memory", "Memory usage", NULL, 0, false, NULL},
    {"-o", "--ondpu", "On DPU", "<id>", 1, false, validatePositiveInt},
    FLAG_TERMINATOR
};

static FlagDefinition configFlags[] =
{
    {"-i", "--init", "Initialize config", NULL, 0, false, NULL},
    {"-r", "--reset", "Reset config", NULL, 0, false, NULL},
    {"-c", "--config", "Show config", NULL, 0, false, NULL},
    {"-s", "--specifics", "Show specifics", NULL, 0, false, NULL},
    {"-d", "--dataset", "Show dataset", "<dataset>", 1, false, validateDataset},
    FLAG_TERMINATOR
};

static FlagDefinition setFlags[] =
{
    {"-Po", "--points", "Set number of points", "<count>", 1, false, validatePositiveInt},
    {"-m", "--mincoord", "Set min coordinate", "<value>", 1, false, validateFloat},
    {"-M", "--maxcoord", "Set max coordinate", "<value>", 1, false, validateFloat},
    {"-Pi", "--pim", "Set PIM value", "<value>", 1, false, validatePositiveInt},
    {"-d", "--dimensions", "Set dimensions", "<value>", 1, false, validatePositiveInt},
    {"-a", "--alpha", "Set alpha", "<value>", 1, false, validateFloat},
    {"-b", "--beta", "Set beta", "<value>", 1, false, validateFloat},
    {"-l", "--leafwrap", "Set leaf wrap threshold", "<value>", 1, false, validatePositiveInt},
    {"-o", "--oversample", "Set oversampling rate", "<value>", 1, false, validatePositiveInt},
    {"-s", "--sketch", "Set sketch height", "<value>", 1, false, validatePositiveInt},
    {"-c", "--chunk", "Set chunk size", "<value>", 1, false, validatePositiveInt},
    FLAG_TERMINATOR
};

static FlagDefinition* findFlag(FlagDefinition* flags, const char* flagName)
{
    logMessage("Searching for flag", DEBUG);

    if(!flags || !flagName)
    {
        logMessage("Invalid flag search input", ERROR);
        return NULL;
    }

    for(FlagDefinition* f = flags; f->shortFlag != NULL; ++f)
    {
        if((f->shortFlag && strcmp(flagName, f->shortFlag) == 0) || (f->longFlag && strcmp(flagName, f->longFlag) == 0))
        {
            logMessage("Flag matched", DEBUG);
            return f;
        }
    }

    logMessage("Flag not found", DEBUG);
    return NULL;
}

static void* parseBuildCommand(char** argv, int argc)
{
    BuildContext* ctx = (BuildContext*)malloc(sizeof(BuildContext));
    if(!ctx)
        return NULL;

    memset(ctx, 0, sizeof(BuildContext));
    ctx->type = CHIP;
    ctx->dataset = DEFAULT10MLN;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(buildFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -c / --chip
        if(strcmp(flag->shortFlag, buildFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, buildFlags[0].longFlag) == 0)
            ctx->type = CHIP;
        // Flag -p / --pim
        else if(strcmp(flag->shortFlag, buildFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, buildFlags[1].longFlag) == 0)
            ctx->type = PIM;
        // Flag -d / --dataset
        else if(strcmp(flag->shortFlag, buildFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, buildFlags[2].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                char* datasetStr = argv[i + 1];
                if(validateDataset(&datasetStr, 1))
                    ctx->dataset = stringToDataset(datasetStr);

                i += flag->argCount;
            }
        }
    }

    return ctx;
}

static void freeBuildContext(void* context)
{
    free(context);
}

static void* parseInsertCommand(char** argv, int argc)
{
    InsertContext* ctx = (InsertContext*)malloc(sizeof(InsertContext));
    if(!ctx)
        return NULL;

    memset(ctx, 0, sizeof(InsertContext));
    ctx->source = INSERT_RANDOM;
    ctx->count = 100;
    ctx->offsetMultiplier = 0;
    ctx->datasetType = DEFAULT1MLN;
    ctx->point = NULL;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(insertFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -f / --file
        if(strcmp(flag->shortFlag, insertFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, insertFlags[0].longFlag) == 0)
        {
            ctx->source = INSERT_FROM_FILE;
            if(i + 1 < argc && isdigit(argv[i + 1][0]))
                ctx->count = atoi(argv[++i]);
        }
        // Flag -r / --random
        else if(strcmp(flag->shortFlag, insertFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, insertFlags[1].longFlag) == 0)
        {
            ctx->source = INSERT_RANDOM;
            if(i + 1 < argc && isdigit(argv[i + 1][0]))
                ctx->count = atoi(argv[++i]);
        }
        // Flag -c / --coordinates
        else if(strcmp(flag->shortFlag, insertFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, insertFlags[2].longFlag) == 0)
        {
            ctx->source = INSERT_COORDINATES;
            if(i + getDimensions() >= argc)
            {
                free(ctx);
                return NULL;
            }

            ctx->point = (point*)malloc(sizeof(point));
            if(!ctx->point)
            {
                free(ctx);
                return NULL;
            }

            ctx->point->coords = (float*)malloc(getDimensions() * sizeof(float));
            if(!ctx->point->coords)
            {
                free(ctx->point);
                free(ctx);
                return NULL;
            }

            for(uint8_t d = 0; d < getDimensions(); ++d)
                ctx->point->coords[d] = atof(argv[i + 1 + d]);

            i += getDimensions();
        }
        // Flag -o / --offset
        else if(strcmp(flag->shortFlag, insertFlags[3].shortFlag) == 0 || strcmp(flag->longFlag, insertFlags[3].longFlag) == 0)
        {
            if(i + 1 < argc)
                ctx->offsetMultiplier = atoi(argv[++i]);
        }
        // Flag -d / --dataset
        else if(strcmp(flag->shortFlag, insertFlags[4].shortFlag) == 0 || strcmp(flag->longFlag, insertFlags[4].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                char* datasetStr = argv[i + 1];
                if(validateDataset(&datasetStr, 1))
                    ctx->datasetType = stringToDataset(datasetStr);

                i += flag->argCount;
            }
        }
    }

    return ctx;
}

static void freeInsertContext(void* context)
{
    InsertContext* ctx = (InsertContext*)context;
    if(ctx)
    {
        if(ctx->point)
        {
            if(ctx->point->coords)
                free(ctx->point->coords);

            free(ctx->point);
        }

        free(ctx);
    }
}

static void* parseDeleteCommand(char** argv, int argc)
{
    DeleteContext* ctx = (DeleteContext*)malloc(sizeof(DeleteContext));
    if(!ctx)
        return NULL;

    memset(ctx, 0, sizeof(DeleteContext));

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(deleteFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -c / --coordinates
        if(strcmp(flag->shortFlag, deleteFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, deleteFlags[0].longFlag) == 0)
        {
            ctx->type = DELETE_COORDINATES;

            int j = i + 1;
            uint32_t coordCount = 0;
            while(j < argc && (isdigit(argv[j][0]) || argv[j][0] == '-' || argv[j][0] == '.'))
            {
                coordCount++;
                j++;
            }

            if(coordCount == 0 || coordCount % getDimensions() != 0)
            {
                free(ctx);
                return NULL;
            }

            ctx->count = coordCount / getDimensions();
            ctx->point = (point**)malloc(ctx->count * sizeof(point*));
            if(!ctx->point)
            {
                free(ctx);
                return NULL;
            }

            for(uint32_t p = 0; p < ctx->count; ++p)
            {
                ctx->point[p] = (point*)malloc(sizeof(point));
                if(!ctx->point[p])
                {
                    for(uint32_t k = 0; k < p; ++k)
                        free(ctx->point[k]);

                    free(ctx->point);
                    free(ctx);

                    return NULL;
                }

                ctx->point[p]->coords = (float*)malloc(getDimensions() * sizeof(float));
                if(!ctx->point[p]->coords)
                {
                    free(ctx->point[p]);

                    for(uint32_t k = 0; k < p; ++k)
                        free(ctx->point[k]);

                    free(ctx->point);
                    free(ctx);

                    return NULL;
                }

                for(uint8_t d = 0; d < getDimensions(); ++d)
                    ctx->point[p]->coords[d] = atof(argv[i + 1 + p * getDimensions() + d]);
            }

            i += coordCount;
        }
        // Flag -r / --random
        else if(strcmp(flag->shortFlag, deleteFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, deleteFlags[1].longFlag) == 0)
        {
            ctx->type = DELETE_RANDOM;
            if(i + 1 < argc && isdigit(argv[i + 1][0]))
                ctx->count = atoi(argv[++i]);
            else
            {
                free(ctx);
                return NULL;
            }
        }
        // Flag -l / --leaf
        else if(strcmp(flag->shortFlag, deleteFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, deleteFlags[2].longFlag) == 0)
        {
            ctx->type = DELETE_LEAF;
            if(i + 1 < argc && isdigit(argv[i + 1][0]))
                ctx->leafCount = atoi(argv[++i]);
            else
                ctx->leafCount = 1;

            ctx->count = ctx->leafCount;
        }
    }

    return ctx;
}

static void freeDeleteContext(void* context)
{
    DeleteContext* ctx = (DeleteContext*)context;
    if(ctx)
    {
        if(ctx->point)
        {
            for(uint32_t i = 0; i < ctx->count; ++i)
                if(ctx->point[i])
                {
                    if(ctx->point[i]->coords)
                        free(ctx->point[i]->coords);
                    free(ctx->point[i]);
                }

            free(ctx->point);
        }

        free(ctx);
    }
}

static void* parseKNNCommand(char** argv, int argc)
{
    logMessage("Parsing KNN command", DEBUG);

    KNNContext* ctx = (KNNContext*)calloc(1, sizeof(KNNContext));
    if(!ctx)
        return NULL;

    ctx->k = 1;
    ctx->approximate = false;
    ctx->epsilon = 0.0f;
    ctx->source = KNN_RANDOM;
    ctx->queryCount = 1;
    ctx->queries = NULL;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(knnFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -k / --k
        if(strcmp(flag->shortFlag, knnFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, knnFlags[0].longFlag) == 0)
        {
            if(i + 1 < argc)
                ctx->k = atoi(argv[++i]);
        }
        // Flag -a / --approximate
        else if(strcmp(flag->shortFlag, knnFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, knnFlags[1].longFlag) == 0)
        {
            ctx->approximate = true;
            if(i + 1 < argc && (isdigit(argv[i+1][0]) || argv[i+1][0] == '.'))
                ctx->epsilon = atof(argv[++i]);
            else
                ctx->epsilon = 0.1f;
        }
        // Flag -c / --coordinates
        else if(strcmp(flag->shortFlag, knnFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, knnFlags[2].longFlag) == 0)
        {
            ctx->source = KNN_RANDOM;
            if(i + 1 < argc)
                ctx->queryCount = atoi(argv[++i]);
        }
        // Flag -r / --random
        else if(strcmp(flag->shortFlag, knnFlags[3].shortFlag) == 0 || strcmp(flag->longFlag, knnFlags[3].longFlag) == 0)
        {
            ctx->source = KNN_COORDINATES;

            int remaining = argc - (i + 1);
            if(remaining >= getDimensions())
            {
                ctx->queryCount = 1;
                ctx->queries = (point**)malloc(sizeof(point*));
                ctx->queries[0] = (point*)malloc(sizeof(point));
                ctx->queries[0]->coords = (float*)malloc(getDimensions() * sizeof(float));

                for(uint8_t d = 0; d < getDimensions(); ++d)
                    ctx->queries[0]->coords[d] = atof(argv[i + 1 + d]);

                i += getDimensions();
            }
        }
    }

    return ctx;
}

static void freeKNNContext(void* context)
{
    KNNContext* ctx = (KNNContext*)context;
    if(ctx)
    {
        if(ctx->queries)
        {
            for(uint32_t i = 0; i < ctx->queryCount; ++i)
            {
                if(ctx->queries[i])
                {
                    if(ctx->queries[i]->coords)
                        free(ctx->queries[i]->coords);
                    free(ctx->queries[i]);
                }
            }

            free(ctx->queries);
        }

        free(ctx);
    }
}

static void* parseRangeCommand(char** argv, int argc)
{
    logMessage("Parsing RANGE command", DEBUG);

    RangeContext* ctx = (RangeContext*)malloc(sizeof(RangeContext));
    if(!ctx)
        return NULL;

    memset(ctx, 0, sizeof(RangeContext));
    ctx->source = RANGE_RANDOM;
    ctx->queryCount = 1;
    ctx->queries = NULL;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(rangeFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -c / --coordinates
        if(strcmp(flag->shortFlag, rangeFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, rangeFlags[0].longFlag) == 0)
        {
            ctx->source = RANGE_COORDINATES;

            int j = i + 1;
            uint32_t coordCount = 0;
            while(j < argc && (isdigit(argv[j][0]) || argv[j][0] == '-' || argv[j][0] == '.'))
            {
                ++coordCount;
                ++j;
            }

            uint8_t dims = getDimensions();
            uint32_t coordsPerQuery = 2 * dims;

            if(coordCount == 0 || coordCount % coordsPerQuery != 0)
            {
                free(ctx);
                return NULL;
            }

            ctx->queryCount = coordCount / coordsPerQuery;
            ctx->queries = (float**)malloc(ctx->queryCount * sizeof(float*));

            if(!ctx->queries)
            {
                free(ctx);
                return NULL;
            }

            for(uint32_t q = 0; q < ctx->queryCount; ++q)
            {
                ctx->queries[q] = (float*)malloc(2 * dims * sizeof(float));
                if(!ctx->queries[q])
                {
                    for(uint32_t k = 0; k < q; ++k)
                        free(ctx->queries[k]);

                    free(ctx->queries);
                    free(ctx);

                    return NULL;
                }

                for(uint8_t d = 0; d < 2 * dims; ++d)
                    ctx->queries[q][d] = (float)atof(argv[i + 1 + q * coordsPerQuery + d]);
            }

            i += coordCount;
        }
        // Flag -r / --random
        else if(strcmp(flag->shortFlag, rangeFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, rangeFlags[1].longFlag) == 0)
        {
            ctx->source = RANGE_RANDOM;
            if(i + 1 < argc)
                ctx->queryCount = atoi(argv[++i]);
        }
    }

    return ctx;
}

static void freeRangeContext(void* context)
{
    RangeContext* ctx = (RangeContext*)context;
    if(ctx)
    {
        if(ctx->queries)
        {
            for(uint32_t i = 0; i < ctx->queryCount; ++i)
                free(ctx->queries[i]);
            free(ctx->queries);
        }
        free(ctx);
    }
}

static void* parseDPCCommand(char** argv, int argc)
{
    logMessage("Parsing DPC command", DEBUG);

    DPCContext* ctx = (DPCContext*)malloc(sizeof(DPCContext));
    if(!ctx)
        return NULL;

    memset(ctx, 0, sizeof(DPCContext));
    ctx->kNeighbors = 10;
    ctx->minDensityThreshold = 0.5f;
    ctx->minDeltaThreshold = 0.5f;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(dpcFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -k / --knn
        if(strcmp(flag->shortFlag, dpcFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, dpcFlags[0].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                ctx->kNeighbors = atoi(argv[++i]);
                if(ctx->kNeighbors == 0)
                    ctx->kNeighbors = 1;
            }
        }
        // Flag -d / --density
        else if(strcmp(flag->shortFlag, dpcFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, dpcFlags[1].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                ctx->minDensityThreshold = atof(argv[++i]);
                if(ctx->minDensityThreshold < 0.0f)
                    ctx->minDensityThreshold = 0.0f;
                if(ctx->minDensityThreshold > 1.0f)
                    ctx->minDensityThreshold = 1.0f;
            }
        }
        // Flag -D / --delta
        else if(strcmp(flag->shortFlag, dpcFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, dpcFlags[2].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                ctx->minDeltaThreshold = atof(argv[++i]);
                if(ctx->minDeltaThreshold < 0.0f)
                    ctx->minDeltaThreshold = 0.0f;
                if(ctx->minDeltaThreshold > 1.0f)
                    ctx->minDeltaThreshold = 1.0f;
            }
        }
    }

    return ctx;
}

static void freeDPCContext(void* context)
{
    free(context);
}

static void* parseDBSCANCommand(char** argv, int argc)
{
    logMessage("Parsing DBSCAN command", DEBUG);

    DBSCANContext* ctx = (DBSCANContext*)malloc(sizeof(DBSCANContext));
    if(!ctx)
        return NULL;

    memset(ctx, 0, sizeof(DBSCANContext));
    ctx->epsilon = 0.0f;
    ctx->minPts = 0;
    ctx->gridCellSize = 0.0f;

    bool hasEpsilon = false;
    bool hasMinPts = false;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(dbscanFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -e / --epsilon
        if(strcmp(flag->shortFlag, dbscanFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, dbscanFlags[0].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                ctx->epsilon = atof(argv[++i]);
                hasEpsilon = true;
            }
        }
        // Flag -m / --minpts
        else if(strcmp(flag->shortFlag, dbscanFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, dbscanFlags[1].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                ctx->minPts = atoi(argv[++i]);
                hasMinPts = true;
            }
        }
        // Flag -g / --gridsize
        else if(strcmp(flag->shortFlag, dbscanFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, dbscanFlags[2].longFlag) == 0)
        {
            if(i + 1 < argc)
                ctx->gridCellSize = atof(argv[++i]);
        }
    }

    if(!hasEpsilon || !hasMinPts)
    {
        logMessage("DBSCAN requires both -e and -m parameters", ERROR);
        free(ctx);
        return NULL;
    }

    if(ctx->epsilon <= 0.0f)
    {
        logMessage("Epsilon must be positive", ERROR);
        free(ctx);
        return NULL;
    }

    if(ctx->minPts == 0)
    {
        logMessage("minPts must be greater than 0", ERROR);
        free(ctx);
        return NULL;
    }

    if(ctx->gridCellSize <= 0.0f)
        ctx->gridCellSize = ctx->epsilon;

    return ctx;
}

static void freeDBSCANContext(void* context)
{
    free(context);
}

static void* parseInfoCommand(char** argv, int argc)
{
    PrintOptions* opt = (PrintOptions*)malloc(sizeof(PrintOptions));
    if(!opt)
        return NULL;

    opt->style = COMPACT;
    opt->dpuId = 0;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(infoFlags, argv[i]);
        if(!flag) continue;

        // Flag -c / --compact
        if(strcmp(flag->shortFlag, infoFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[0].longFlag) == 0)
            opt->style = COMPACT;
        // Flag -c / --tree
        else if(strcmp(flag->shortFlag, infoFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[1].longFlag) == 0)
            opt->style = TREE;
        // Flag -d / --detailed
        else if(strcmp(flag->shortFlag, infoFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[2].longFlag) == 0)
            opt->style = DETAILED;
        // Flag -v / --validate
        else if(strcmp(flag->shortFlag, infoFlags[3].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[3].longFlag) == 0)
            opt->style = VALIDATE;
        // Flag -a / --approximate
        else if(strcmp(flag->shortFlag, infoFlags[4].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[4].longFlag) == 0)
            opt->style = APPROXIMATE;
        // Flag -r / --replicas
        else if(strcmp(flag->shortFlag, infoFlags[5].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[5].longFlag) == 0)
            opt->style = REPLICAS;
        // Flag -s / --stats
        else if(strcmp(flag->shortFlag, infoFlags[6].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[6].longFlag) == 0)
            opt->style = STATS;
        // Flag -m / --memory
        else if(strcmp(flag->shortFlag, infoFlags[7].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[7].longFlag) == 0)
            opt->style = MEMORY;
        // Flag -o / -ondpu
        else if(strcmp(flag->shortFlag, infoFlags[8].shortFlag) == 0 || strcmp(flag->longFlag, infoFlags[8].longFlag) == 0)
        {
            if(i + 1 < argc)
            {
                opt->style = ONDPU;
                opt->dpuId = atoi(argv[++i]);
            }
        }
    }

    return opt;
}

static void freeInfoContext(void* context)
{
    free(context);
}

static void* parseConfigCommand(char** argv, int argc)
{
    ConfigContext* ctx = (ConfigContext*)malloc(sizeof(ConfigContext));
    if(!ctx)
        return NULL;

    ctx->type = CONFIGURATION;
    ctx->dataset = DEFAULT100;

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(configFlags, argv[i]);
        if(!flag)
            continue;

        // Flag -i / --init
        if(strcmp(flag->shortFlag, configFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, configFlags[0].longFlag) == 0)
            ctx->type = INIT;
        // Flag -r / --reset
        else if(strcmp(flag->shortFlag, configFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, configFlags[1].longFlag) == 0)
            ctx->type = RESET;
        // Flag -c / --config
        else if(strcmp(flag->shortFlag, configFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, configFlags[2].longFlag) == 0)
            ctx->type = CONFIGURATION;
        // Flag -s / --specifics
        else if(strcmp(flag->shortFlag, configFlags[3].shortFlag) == 0 || strcmp(flag->longFlag, configFlags[3].longFlag) == 0)
            ctx->type = SPECIFICS;
        // Flag -d / --dataset
        else if(strcmp(flag->shortFlag, configFlags[4].shortFlag) == 0 || strcmp(flag->longFlag, configFlags[4].longFlag) == 0)
        {
            ctx->type = DATASET;
            if(i + 1 < argc)
            {
                char* datasetStr = argv[i + 1];
                if(validateDataset(&datasetStr, 1))
                {
                    ctx->dataset = stringToDataset(datasetStr);
                    ++i;
                }
            }
        }
    }

    return ctx;
}

static void freeConfigContext(void* context)
{
    free(context);
}

static void* parseSetCommand(char** argv, int argc)
{
    SetContext* ctx = (SetContext*)malloc(sizeof(SetContext));
    if(!ctx)
        return NULL;

    if(argc < 3)
    {
        free(ctx);
        return NULL;
    }

    for(int i = 1; i < argc; ++i)
    {
        FlagDefinition* flag = findFlag(setFlags, argv[i]);
        if(!flag) continue;

        // Flag -Po / --points
        if(strcmp(flag->shortFlag, setFlags[0].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[0].longFlag) == 0)
        {
            ctx->type = NPOINT;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint32_t));
                if(ctx->value) *(uint32_t*)ctx->value = atoi(argv[++i]);
            }
        }
        // Flag -m / --mincoord
        else if(strcmp(flag->shortFlag, setFlags[1].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[1].longFlag) == 0)
        {
            ctx->type = MINCOORD;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(double));
                if(ctx->value) *(double*)ctx->value = strtod(argv[++i], NULL);
            }
        }
        // Flag -M / --maxcoord
        else if(strcmp(flag->shortFlag, setFlags[2].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[2].longFlag) == 0)
        {
            ctx->type = MAXCOORD;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(double));
                if(ctx->value) *(double*)ctx->value = strtod(argv[++i], NULL);
            }
        }
        // Flag -Pi / --pim
        else if(strcmp(flag->shortFlag, setFlags[3].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[3].longFlag) == 0)
        {
            ctx->type = NPIM;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint8_t));
                if(ctx->value) *(uint8_t*)ctx->value = atoi(argv[++i]);
            }
        }
        // Flag -d / --dimensions
        else if(strcmp(flag->shortFlag, setFlags[4].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[4].longFlag) == 0)
        {
            ctx->type = DIMENSIONS;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint8_t));
                if(ctx->value) *(uint8_t*)ctx->value = atoi(argv[++i]);
            }
        }
        // Flag -a / --alpha
        else if(strcmp(flag->shortFlag, setFlags[5].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[5].longFlag) == 0)
        {
            ctx->type = ALPHA;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(float));
                if(ctx->value) *(float*)ctx->value = atof(argv[++i]);
            }
        }
        // Flag -b / --beta
        else if(strcmp(flag->shortFlag, setFlags[6].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[6].longFlag) == 0)
        {
            ctx->type = BETA;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(float));
                if(ctx->value) *(float*)ctx->value = atof(argv[++i]);
            }
        }
        // Flag -l / --leaf
        else if(strcmp(flag->shortFlag, setFlags[7].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[7].longFlag) == 0)
        {
            ctx->type = LEAFWRAPTHRESHOLD;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint16_t));
                if(ctx->value) *(uint16_t*)ctx->value = atoi(argv[++i]);
            }
        }
        // Flag -o / --oversampling
        else if(strcmp(flag->shortFlag, setFlags[8].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[8].longFlag) == 0)
        {
            ctx->type = OVERSAMPLINGRATE;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint16_t));
                if(ctx->value) *(uint16_t*)ctx->value = atoi(argv[++i]);
            }
        }
        // Flag -s / --sketch
        else if(strcmp(flag->shortFlag, setFlags[9].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[9].longFlag) == 0)
        {
            ctx->type = SKETCHHEIGHT;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint8_t));
                if(ctx->value) *(uint8_t*)ctx->value = atoi(argv[++i]);
            }
        }
        // Flag -c / --chunk
        else if(strcmp(flag->shortFlag, setFlags[10].shortFlag) == 0 || strcmp(flag->longFlag, setFlags[10].longFlag) == 0)
        {
            ctx->type = CHUNKSIZE;
            if(i + 1 < argc)
            {
                ctx->value = malloc(sizeof(uint16_t));
                if(ctx->value) *(uint16_t*)ctx->value = atoi(argv[++i]);
            }
        }
        else
        {
            free(ctx);
            return NULL;
        }

        break;
    }

    if(!ctx->value)
    {
        free(ctx);
        return NULL;
    }

    return ctx;
}

static void freeSetContext(void* context)
{
    SetContext* ctx = (SetContext*)context;
    if(ctx)
    {
        if(ctx->value)
            free(ctx->value);

        free(ctx);
    }
}

CommandParser buildParser = {BUILD, parseBuildCommand, freeBuildContext};
CommandParser insertParser = {INSERT, parseInsertCommand, freeInsertContext};
CommandParser deleteParser = {DELETE, parseDeleteCommand, freeDeleteContext};
CommandParser knnParser = {KNN, parseKNNCommand, freeKNNContext};
CommandParser rangeParser = {RANGE, parseRangeCommand, freeRangeContext};
CommandParser dpcParser = {CLUSTER_DPC, parseDPCCommand, freeDPCContext};
CommandParser dbscanParser = {CLUSTER_DBSCAN, parseDBSCANCommand, freeDBSCANContext};
CommandParser infoParser = {INFOS, parseInfoCommand, freeInfoContext};
CommandParser configParser = {CONFIG, parseConfigCommand, freeConfigContext};
CommandParser setParser = {SET, parseSetCommand, freeSetContext};

static void* parseEmptyCommand(char** argv, int argc)
{
    return malloc(1);
}

static void freeEmptyContext(void* context)
{
    free(context);
}

CommandParser benchmarkParser = {BENCHMARKS, parseEmptyCommand, freeEmptyContext};

FlagDefinition* getBuildFlags()
{
    return buildFlags;
}

FlagDefinition* getInsertFlags()
{
    return insertFlags;
}

FlagDefinition* getDeleteFlags()
{
    return deleteFlags;
}

FlagDefinition* getKnnFlags()
{
    return knnFlags;
}

FlagDefinition* getRangeFlags()
{
    return rangeFlags;
}

FlagDefinition* getDpcFlags()
{
    return dpcFlags;
}

FlagDefinition* getDbscanFlags()
{
    return dbscanFlags;
}

FlagDefinition* getInfoFlags()
{
    return infoFlags;
}

FlagDefinition* getConfigFlags()
{
    return configFlags;
}

FlagDefinition* getSetFlags()
{
    return setFlags;
}

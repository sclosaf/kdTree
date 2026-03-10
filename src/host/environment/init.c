#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <limits.h>
#include <libgen.h>

#include "kdTree/utils.h"

#include "environment/init.h"
#include "environment/constants.h"
#include "management/logging.h"

static Config config =
{
    .nPoint = DEFAULT_NPOINT,
    .nPim = DEFAULT_NPIM,
    .dimensions = DEFAULT_DIMENSIONS,
    .alpha = DEFAULT_ALPHA,
    .beta = DEFAULT_BETA,
    .leafWrapThreshold = DEFAULT_LEAF_WRAP_THRESHOLD,
    .oversamplingRate = DEFAULT_OVERSAMPLING_RATE,
    .sketchHeight = DEFAULT_SKETCH_HEIGHT,
    .chunkSize = DEFAULT_CHUNK_SIZE
};

static Data data =
{
    .tree = NULL
};

static bool configInitialized = false;

const Config* getConfig()
{
    if(!configInitialized)
        initConfig();

    return &config;
}

char* getProjectRoot()
{
    static char root[PATH_MAX];
    char path[PATH_MAX];
    ssize_t len;

    len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if(len == -1)
        return NULL;

    path[len] = '\0';

    if(dirname(path) == NULL)
        return NULL;

    char temp[PATH_MAX];
    strncpy(temp, path, sizeof(temp));
    temp[sizeof(temp) - 1] = '\0';

    if(dirname(temp) == NULL)
        return NULL;

    strncpy(root, temp, sizeof(root));
    root[sizeof(root) - 1] = '\0';

    return root;
}

static char* trim(char* str)
{
    char* end;

    while(*str == ' ' || *str == '\t')
        ++str;

    if(*str == 0)
        return str;

    end = str + strlen(str) - 1;
    while(end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r'))
        --end;

    *(end + 1) = '\0';
    return str;
}

static int parseLine(char* line, char** key, char** value)
{
    char* equals = strchr(line, '=');
    if(equals == NULL)
        return -1;

    *equals = '\0';
    *key = trim(line);
    *value = trim(equals + 1);

    return 0;
}

static void setValue(const char* key, const char* value)
{
    char* endptr;

    if(strcmp(key, "nPoint") == 0)
        config.nPoint = strtoul(value, &endptr, 10);
    else if(strcmp(key, "nPim") == 0)
        config.nPim = strtoul(value, &endptr, 10);
    else if(strcmp(key, "dimensions") == 0)
        config.dimensions = strtoul(value, &endptr, 10);
    else if(strcmp(key, "alpha") == 0)
        config.alpha = strtof(value, &endptr);
    else if(strcmp(key, "beta") == 0)
        config.beta = strtof(value, &endptr);
    else if(strcmp(key, "leafWrapThreshold") == 0)
        config.leafWrapThreshold = strtoul(value, &endptr, 10);
    else if(strcmp(key, "oversamplingRate") == 0)
        config.oversamplingRate = strtoul(value, &endptr, 10);
    else if(strcmp(key, "sketchHeight") == 0)
        config.sketchHeight = strtoul(value, &endptr, 10);
    else if(strcmp(key, "chunkSize") == 0)
        config.chunkSize = strtoul(value, &endptr, 10);
    else if(strcmp(key, "stream") == 0)
    {
        if(strlen(value) == 0 || strcmp(value, "stdout") == 0)
            setStream(stdout);
        else
        {
            char logPath[512];
            const char* root = getProjectRoot();

            if(root != NULL)
            {
                snprintf(logPath, sizeof(logPath), "%s/log/%s", root, value);

                FILE* fileStream = fopen(logPath, "a");
                if(fileStream != NULL)
                    setStream(fileStream);
                else
                    setStream(stdout);
            }
            else
                setStream(stdout);
        }
    }
    else if(strcmp(key, "severity") == 0)
    {
        if(strcmp(value, "ERROR") == 0)
            setVerbosity(ERROR);\
        else if(strcmp(value, "WARNING") == 0)
            setVerbosity(WARNING);
        else if(strcmp(value, "DEBUG") == 0)
            setVerbosity(DEBUG);
    }
}

void printConfig()
{
    printf("Loaded configuration:\n");
    printf("  nPoint = %u\n", config.nPoint);
    printf("  nPim = %u\n", config.nPim);
    printf("  dimensions = %u\n", config.dimensions);
    printf("  alpha = %.2f\n", config.alpha);
    printf("  beta = %.2f\n", config.beta);
    printf("  leafWrapThreshold = %u\n", config.leafWrapThreshold);
    printf("  oversamplingRate = %u\n", config.oversamplingRate);
    printf("  sketchHeight = %u\n", config.sketchHeight);
    printf("  chunkSize = %u\n", config.chunkSize);
}

void initConfig()
{
    char path[512];
    FILE* file;
    char line[128];
    char* root;
    int line_num = 0;

    if(configInitialized)
        return;

    root = getProjectRoot();
    if(root == NULL)
    {
        configInitialized = true;
        return;
    }

    snprintf(path, sizeof(path), "%s/.env", root);

    file = fopen(path, "r");
    if(file == NULL)
    {
        configInitialized = true;
        return;
    }

    while(fgets(line, sizeof(line), file) != NULL)
    {
        char* key;
        char* value;
        line_num++;

        if(line[0] == '\n' || line[0] == '\r' || line[0] == '#')
            continue;

        if(parseLine(line, &key, &value) == 0)
            setValue(key, value);
    }

    fclose(file);
    configInitialized = true;

    return;
}

void resetConfig()
{
    config.nPoint = DEFAULT_NPOINT;
    config.nPim = DEFAULT_NPIM;
    config.dimensions = DEFAULT_DIMENSIONS;
    config.alpha = DEFAULT_ALPHA;
    config.beta = DEFAULT_BETA;
    config.leafWrapThreshold = DEFAULT_LEAF_WRAP_THRESHOLD;
    config.oversamplingRate = DEFAULT_OVERSAMPLING_RATE;
    config.sketchHeight = DEFAULT_SKETCH_HEIGHT;
    config.chunkSize = DEFAULT_CHUNK_SIZE;
    configInitialized = true;
}

Data* getData()
{
    return &data;
}

void freeData()
{
    if(data.tree)
    {
        freeKDTree(data.tree->root);
        free(data.tree);

        data.tree = NULL;
    }
}

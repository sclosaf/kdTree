#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <limits.h>
#include <libgen.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "host/kdTree/utils.h"
#include "host/kdTree/free.h"

#include "host/environment/init.h"
#include "host/environment/constants.h"

#include "host/management/logging.h"

static Config config =
{
    .nPoint = DEFAULT_NPOINT,
    .minCoord = DEFAULT_MIN_COORD,
    .maxCoord = DEFAULT_MAX_COORD,
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
    .tree = NULL,
    .map = NULL
};

static char* trim(char* str)
{
    logMessage("Trimming whitespace from string", DEBUG);

    char* end;

    while(*str == ' ' || *str == '\t')
        ++str;

    if(*str == 0)
    {
        logMessage("String is empty after trimming", DEBUG);
        return str;
    }

    end = str + strlen(str) - 1;
    while(end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r'))
        --end;

    *(end + 1) = '\0';
    logMessage("String trimmed successfully", DEBUG);

    return str;
}

static int parseLine(char* line, char** key, char** value)
{
    logMessage("Parsing configuration line", DEBUG);

    char* equals = strchr(line, '=');
    if(equals == NULL)
    {
        logMessage("No '=' found in line, skipping", DEBUG);
        return -1;
    }

    *equals = '\0';
    *key = trim(line);
    *value = trim(equals + 1);

    logMessage("Line parsed successfully", DEBUG);

    return 0;
}

static void setValue(const char* key, const char* value)
{
    char* endptr;
    char msg[2048];

    logMessage("Setting configuration value", DEBUG);
    snprintf(msg, sizeof(msg), "Key: %s, Value: %s", key, value);
    logMessage(msg, DEBUG);

    if(strcmp(key, "nPoint") == 0)
    {
        config.nPoint = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "nPoint set to %u", config.nPoint);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "minCoord") == 0)
    {
        config.minCoord = strtod(value, &endptr);
        snprintf(msg, sizeof(msg), "minCoord set to %.4f", config.minCoord);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "maxCoord") == 0)
    {
        config.maxCoord = strtod(value, &endptr);
        snprintf(msg, sizeof(msg), "maxCoord set to %.4f", config.maxCoord);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "nPim") == 0)
    {
        config.nPim = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "nPim set to %u", config.nPim);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "dimensions") == 0)
    {
        config.dimensions = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "dimensions set to %u", config.dimensions);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "alpha") == 0)
    {
        config.alpha = strtof(value, &endptr);
        snprintf(msg, sizeof(msg), "alpha set to %.2f", config.alpha);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "beta") == 0)
    {
        config.beta = strtof(value, &endptr);
        snprintf(msg, sizeof(msg), "beta set to %.2f", config.beta);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "leafWrapThreshold") == 0)
    {
        config.leafWrapThreshold = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "leafWrapThreshold set to %u", config.leafWrapThreshold);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "oversamplingRate") == 0)
    {
        config.oversamplingRate = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "oversamplingRate set to %u", config.oversamplingRate);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "sketchHeight") == 0)
    {
        config.sketchHeight = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "sketchHeight set to %u", config.sketchHeight);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "chunkSize") == 0)
    {
        config.chunkSize = strtoul(value, &endptr, 10);
        snprintf(msg, sizeof(msg), "chunkSize set to %u", config.chunkSize);
        logMessage(msg, INFO);
    }
    else if(strcmp(key, "stream") == 0)
    {
        logMessage("Configuring log stream", DEBUG);

        char logPath[1024];
        const char* root = getProjectRoot();

        if(strlen(value) == 0 || strcmp(value, "stdout") == 0)
        {
            setStream(stdout);
            logMessage("Log stream set to stdout", INFO);
        }
        else if(root != NULL)
        {
            char dirPath[512];
            snprintf(dirPath, sizeof(dirPath), "%s/log", root);

            mkdir(dirPath, 0755);

            snprintf(logPath, sizeof(logPath), "%s/%s", dirPath, value);

            snprintf(msg, sizeof(msg), "Attempting to open log file: %s", logPath);
            logMessage(msg, DEBUG);

            FILE* fileStream = fopen(logPath, "w");
            if(fileStream != NULL)
            {
                setStream(fileStream);
                snprintf(msg, sizeof(msg), "Log stream successfully set to file: %s", logPath);
                logMessage(msg, INFO);
            }
            else
            {
                snprintf(msg, sizeof(msg), "Failed to open [%s]. Ensure 'log/' directory exists. Using stdout.", logPath);
                logMessage(msg, ERROR);
                setStream(stdout);
            }
        }
        else
        {
            logMessage("Failed to get project root, using stdout", ERROR);
            setStream(stdout);
        }
    }
    else if(strcmp(key, "severity") == 0)
    {
        logMessage("Configuring log severity", DEBUG);

        if(strcmp(value, "NONE") == 0)
        {
            setVerbosity(NONE);
            logMessage("Log severity set to NONE (no logging)", INFO);
        }
        else if(strcmp(value, "TEST") == 0)
        {
            setVerbosity(TEST);
            logMessage("Log severity set to TEST", INFO);
        }
        else if(strcmp(value, "ERROR") == 0)
        {
            setVerbosity(ERROR);
            logMessage("Log severity set to ERROR", INFO);
        }
        else if(strcmp(value, "DEBUG") == 0)
        {
            setVerbosity(DEBUG);
            logMessage("Log severity set to DEBUG", INFO);
        }
        else if(strcmp(value, "INFO") == 0)
        {
            setVerbosity(INFO);
            logMessage("Log severity set to INFO", INFO);
        }
        else
        {
            snprintf(msg, sizeof(msg), "Unknown severity value: %s, using ERROR", value);
            logMessage(msg, ERROR);
        }
    }
}

char* getProjectRoot()
{
    logMessage("Getting project root directory", DEBUG);

    static char root[PATH_MAX + 512];
    char path[PATH_MAX + 512];
    ssize_t len;

    len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if(len == -1)
    {
        logMessage("Failed to read /proc/self/exe", ERROR);
        return NULL;
    }

    path[len] = '\0';

    char *dir = dirname(path);
    if(dir == NULL)
        return NULL;

    char temp[PATH_MAX + 512];
    strncpy(temp, dir, sizeof(temp) - 1);

    char *parent = dirname(temp);
    if(parent == NULL)
        return NULL;

    strncpy(root, parent, sizeof(root) - 1);
    root[sizeof(root) - 1] = '\0';

    char msg[sizeof(root) + 128];
    snprintf(msg, sizeof(msg), "Project root: %s", root);
    logMessage(msg, INFO);

    return root;
}

void printConfig()
{
    logMessage("Printing current configuration", DEBUG);

    printf("Loaded configuration:\n");
    printf("  nPoint = %u\n", config.nPoint);
    printf("  minCoord = %.4f\n", config.minCoord);
    printf("  maxCoord = %.4f\n", config.maxCoord);
    printf("  nPim = %u\n", config.nPim);
    printf("  dimensions = %u\n", config.dimensions);
    printf("  alpha = %.2f\n", config.alpha);
    printf("  beta = %.2f\n", config.beta);
    printf("  leafWrapThreshold = %u\n", config.leafWrapThreshold);
    printf("  oversamplingRate = %u\n", config.oversamplingRate);
    printf("  sketchHeight = %u\n", config.sketchHeight);
    printf("  chunkSize = %u\n", config.chunkSize);

    logMessage("Configuration printed successfully", DEBUG);
}

void initConfig()
{
    logMessage("Initializing configuration", DEBUG);

    char path[1024];
    FILE* file;
    char line[128];
    char* root;
    int lineNum = 0;
    char msg[2048];

    root = getProjectRoot();
    if(root == NULL)
    {
        logMessage("Failed to get project root, using default configuration", ERROR);
        return;
    }

    snprintf(path, sizeof(path), "%s/.env", root);
    snprintf(msg, sizeof(msg), "Looking for config file: %s", path);
    logMessage(msg, DEBUG);

    file = fopen(path, "r");
    if(file == NULL)
    {
        logMessage("No .env file found, using default configuration", INFO);
        return;
    }

    logMessage("Configuration file opened successfully", DEBUG);

    while(fgets(line, sizeof(line), file) != NULL)
    {
        char* key;
        char* value;
        lineNum++;

        if(line[0] == '\n' || line[0] == '\r' || line[0] == '#')
        {
            snprintf(msg, sizeof(msg), "Skipping line %d (empty or comment)", lineNum);
            logMessage(msg, DEBUG);
            continue;
        }

        snprintf(msg, sizeof(msg), "Processing line %d", lineNum);
        logMessage(msg, DEBUG);

        if(parseLine(line, &key, &value) == 0)
            setValue(key, value);
        else
        {
            snprintf(msg, sizeof(msg), "Failed to parse line %d: %s", lineNum, line);
            logMessage(msg, ERROR);
        }
    }

    fclose(file);

    logMessage("Configuration initialization complete", DEBUG);
}

void resetConfig()
{
    logMessage("Resetting configuration to defaults", DEBUG);

    config.nPoint = DEFAULT_NPOINT;
    config.minCoord = DEFAULT_MIN_COORD;
    config.maxCoord = DEFAULT_MAX_COORD;
    config.nPim = DEFAULT_NPIM;
    config.dimensions = DEFAULT_DIMENSIONS;
    config.alpha = DEFAULT_ALPHA;
    config.beta = DEFAULT_BETA;
    config.leafWrapThreshold = DEFAULT_LEAF_WRAP_THRESHOLD;
    config.oversamplingRate = DEFAULT_OVERSAMPLING_RATE;
    config.sketchHeight = DEFAULT_SKETCH_HEIGHT;
    config.chunkSize = DEFAULT_CHUNK_SIZE;

    char msg[256];
    snprintf(msg, sizeof(msg), "Configuration reset to defaults: nPoint=%u, dimensions=%u", config.nPoint, config.dimensions);
    logMessage(msg, INFO);

    logMessage("Configuration reset complete", DEBUG);
}

Data* getData()
{
    logMessage("Returning data structure pointer", DEBUG);
    return &data;
}

void freeData()
{
    logMessage("Freeing data structures", DEBUG);

    if(data.tree)
    {
        logMessage("Freeing KDTree structure", DEBUG);
        freeKDTree(data.tree->root);
        free(data.tree);
        data.tree = NULL;
        logMessage("KDTree freed successfully", DEBUG);
    }

    if(data.map)
    {
        logMessage("Freeing node location map", DEBUG);
        freeNodeLocationMap();
        free(data.map);
        data.map = NULL;
        logMessage("Node location map freed successfully", DEBUG);
    }

    logMessage("All data structures freed successfully", INFO);
}

uint32_t getNPoint()
{
    return config.nPoint;
}

float getMinCoord()
{
    return config.minCoord;
}

float getMaxCoord()
{
    return config.maxCoord;
}

uint8_t getNPim()
{
    return config.nPim;
}

uint8_t getDimensions()
{
    return config.dimensions;
}

float getAlpha()
{
    return config.alpha;
}

float getBeta()
{
    return config.beta;
}

uint16_t getLeafWrapThreshold()
{
    return config.leafWrapThreshold;
}

uint16_t getOversamplingRate()
{
    return config.oversamplingRate;
}

uint8_t getSketchHeight()
{
    return config.sketchHeight;
}

uint16_t getChunkSize()
{
    return config.chunkSize;
}

void setNPoint(uint32_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting nPoint from %u to %u", config.nPoint, value);
    logMessage(msg, DEBUG);

    config.nPoint = value;

    snprintf(msg, sizeof(msg), "nPoint updated to %u", config.nPoint);
    logMessage(msg, INFO);
}

void setMinCoord(float value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting minCoord from %.4f to %.4f", config.minCoord, value);
    logMessage(msg, DEBUG);

    config.minCoord = value;

    snprintf(msg, sizeof(msg), "minCoord updated to %.4f", config.minCoord);
    logMessage(msg, INFO);
}

void setMaxCoord(float value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting maxCoord from %.4f to %.4f", config.maxCoord, value);
    logMessage(msg, DEBUG);

    config.maxCoord = value;

    snprintf(msg, sizeof(msg), "maxCoord updated to %.4f", config.maxCoord);
    logMessage(msg, INFO);
}

void setNPim(uint8_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting nPim from %u to %u", config.nPim, value);
    logMessage(msg, DEBUG);

    config.nPim = value;

    snprintf(msg, sizeof(msg), "nPim updated to %u", config.nPim);
    logMessage(msg, INFO);
}

void setDimensions(uint8_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting dimensions from %u to %u", config.dimensions, value);
    logMessage(msg, DEBUG);

    config.dimensions = value;

    snprintf(msg, sizeof(msg), "dimensions updated to %u", config.dimensions);
    logMessage(msg, INFO);
}

void setAlpha(float value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting alpha from %.2f to %.2f", config.alpha, value);
    logMessage(msg, DEBUG);

    config.alpha = value;

    snprintf(msg, sizeof(msg), "alpha updated to %.2f", config.alpha);
    logMessage(msg, INFO);
}

void setBeta(float value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting beta from %.2f to %.2f", config.beta, value);
    logMessage(msg, DEBUG);

    config.beta = value;

    snprintf(msg, sizeof(msg), "beta updated to %.2f", config.beta);
    logMessage(msg, INFO);
}

void setLeafWrapThreshold(uint16_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting leafWrapThreshold from %u to %u", config.leafWrapThreshold, value);
    logMessage(msg, DEBUG);

    config.leafWrapThreshold = value;

    snprintf(msg, sizeof(msg), "leafWrapThreshold updated to %u", config.leafWrapThreshold);
    logMessage(msg, INFO);
}

void setOversamplingRate(uint16_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting oversamplingRate from %u to %u", config.oversamplingRate, value);
    logMessage(msg, DEBUG);

    config.oversamplingRate = value;

    snprintf(msg, sizeof(msg), "oversamplingRate updated to %u", config.oversamplingRate);
    logMessage(msg, INFO);
}

void setSketchHeight(uint8_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting sketchHeight from %u to %u", config.sketchHeight, value);
    logMessage(msg, DEBUG);

    config.sketchHeight = value;

    snprintf(msg, sizeof(msg), "sketchHeight updated to %u", config.sketchHeight);
    logMessage(msg, INFO);
}

void setChunkSize(uint16_t value)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Setting chunkSize from %u to %u", config.chunkSize, value);
    logMessage(msg, DEBUG);

    config.chunkSize = value;

    snprintf(msg, sizeof(msg), "chunkSize updated to %u", config.chunkSize);
    logMessage(msg, INFO);
}

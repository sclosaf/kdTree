#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <omp.h>

#include "host/query/knn.h"
#include "host/kdTree/search.h"
#include "host/kdTree/build.h"
#include "host/kdTree/utils.h"
#include "host/kdTree/free.h"

#include "host/environment/init.h"
#include "host/management/logging.h"

typedef struct KNNCandidate
{
    point* p;
    float distance;
} KNNCandidate;

typedef struct KNNCandidateHeap
{
    KNNCandidate* candidates;
    uint32_t size;
    uint32_t capacity;
    uint32_t k;
} KNNCandidateHeap;

static KNNCandidateHeap* createCandidateHeap(uint32_t k)
{
    logMessage("Creating candidate heap", DEBUG);

    KNNCandidateHeap* heap = malloc(sizeof(KNNCandidateHeap));
    if(!heap)
    {
        logMessage("Failed to allocate candidate heap", ERROR);
        return NULL;
    }

    heap->k = k;
    heap->size = 0;
    heap->capacity = k + 16;
    heap->candidates = calloc(heap->capacity, sizeof(KNNCandidate));

    if(!heap->candidates)
    {
        logMessage("Failed to allocate candidates array", ERROR);
        free(heap);
        return NULL;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "Heap created: k=%u, capacity=%u", k, heap->capacity);
    logMessage(msg, DEBUG);

    return heap;
}

static void freeCandidateHeap(KNNCandidateHeap* heap)
{
    if(heap)
    {
        free(heap->candidates);
        free(heap);
    }
}

static int candidateCompare(const void* a, const void* b)
{
    const KNNCandidate* ca = (const KNNCandidate*)a;
    const KNNCandidate* cb = (const KNNCandidate*)b;

    if(ca->distance > cb->distance)
        return -1;

    if(ca->distance < cb->distance)
        return 1;

    return 0;
}

static void heapPush(KNNCandidateHeap* heap, KNNCandidate candidate)
{
    if(heap->size >= heap->capacity)
    {
        heap->capacity *= 2;
        heap->candidates = realloc(heap->candidates, heap->capacity * sizeof(KNNCandidate));
    }

    heap->candidates[heap->size++] = candidate;

    uint32_t index = heap->size - 1;
    while(index > 0)
    {
        uint32_t parent = (index - 1) / 2;
        if(candidateCompare(&heap->candidates[parent], &heap->candidates[index]) >= 0)
            break;

        KNNCandidate tmp = heap->candidates[parent];
        heap->candidates[parent] = heap->candidates[index];
        heap->candidates[index] = tmp;
        index = parent;
    }
}

static KNNCandidate heapPopMax(KNNCandidateHeap* heap)
{
    KNNCandidate result = heap->candidates[0];
    heap->candidates[0] = heap->candidates[--heap->size];

    uint32_t index = 0;
    while(1)
    {
        uint32_t left = 2 * index + 1;
        uint32_t right = 2 * index + 2;
        uint32_t largest = index;

        if(left < heap->size && candidateCompare(&heap->candidates[left], &heap->candidates[largest]) > 0)
            largest = left;

        if(right < heap->size && candidateCompare(&heap->candidates[right], &heap->candidates[largest]) > 0)
            largest = right;

        if(largest == index)
            break;

        KNNCandidate tmp = heap->candidates[index];
        heap->candidates[index] = heap->candidates[largest];
        heap->candidates[largest] = tmp;
        index = largest;
    }

    return result;
}

static float getWorstDistance(KNNCandidateHeap* heap)
{
    if(heap->size < heap->k)
        return INFINITY;

    return heap->candidates[0].distance;
}

static void tryAddCandidate(KNNCandidateHeap* heap, point* p, point* query)
{
    uint8_t dims = getDimensions();
    float dist = 0.0f;

    for(uint8_t d = 0; d < dims; ++d)
    {
        float diff = p->coords[d] - query->coords[d];
        dist += diff * diff;
    }

    char msg[256];

    if(heap->size < heap->k)
    {
        KNNCandidate cand = {
            .p = p,
            .distance = dist
        };
        heapPush(heap, cand);

        snprintf(msg, sizeof(msg), "Candidate added (heap size=%u/%u, dist^2=%.4f)", heap->size, heap->k, dist);
        logMessage(msg, DEBUG);
    }
    else if(dist < getWorstDistance(heap))
    {
        float oldWorst = getWorstDistance(heap);
        heapPopMax(heap);
        KNNCandidate cand = {
            .p = p,
            .distance = dist
        };
        heapPush(heap, cand);

        snprintf(msg, sizeof(msg), "Candidate replaced (dist^2=%.4f, old worst=%.4f, new worst=%.4f)",  dist, oldWorst, getWorstDistance(heap));
        logMessage(msg, DEBUG);
    }
    else
    {
        snprintf(msg, sizeof(msg), "Candidate rejected (dist^2=%.4f >= worst=%.4f)",  dist, getWorstDistance(heap));
        logMessage(msg, DEBUG);
    }
}

static float minDistToSplitPlane(KDNode* node, point* query)
{
    if(!node || node->type != INTERNAL)
        return INFINITY;

    uint8_t dim = node->data.internal.splitDim;
    float splitVal = node->data.internal.splitValue;
    float diff = query->coords[dim] - splitVal;

    return (diff > 0) ? diff : -diff;
}

static bool shouldPrune(KDNode* node, point* query, float radius)
{
    if(!node)
        return true;

    if(radius >= INFINITY)
        return false;

    float minDist = minDistToSplitPlane(node, query);
    return minDist > radius;
}

static void collectPointsFromLeaf(KNNCandidateHeap* heap, KDNode* leaf, point* query)
{
    if(!leaf || leaf->type != LEAF)
        return;

    for(size_t i = 0; i < leaf->data.leaf.pointsCount; ++i)
        tryAddCandidate(heap, &leaf->data.leaf.points[i], query);
}

static void backtrackSearch(KDNode* node, point* query, float* radius, KNNCandidateHeap* heap)
{
    if(!node)
        return;

    *radius = getWorstDistance(heap);

    if(shouldPrune(node, query, *radius))
        return;

    if(node->type == LEAF)
    {
        collectPointsFromLeaf(heap, node, query);
        *radius = getWorstDistance(heap);
        return;
    }

    uint8_t dim = node->data.internal.splitDim;
    float splitVal = node->data.internal.splitValue;
    KDNode* nearChild = (query->coords[dim] < splitVal) ? node->data.internal.left : node->data.internal.right;
    KDNode* farChild = (query->coords[dim] < splitVal) ? node->data.internal.right : node->data.internal.left;

    backtrackSearch(nearChild, query, radius, heap);

    *radius = getWorstDistance(heap);
    float distToPlane = fabs(query->coords[dim] - splitVal);
    if(distToPlane < *radius)
        backtrackSearch(farChild, query, radius, heap);
}

static void singleKNNQuery(point* query, uint32_t k, KDNode* root, KNNResult* result)
{
    char msg[256];

    if(!query || !root || k == 0)
    {
        logMessage("Invalid arguments to singleKNNQuery", ERROR);
        return;
    }

    snprintf(msg, sizeof(msg), "Starting exact kNN query: k=%u", k);
    logMessage(msg, INFO);

    KNNCandidateHeap* heap = createCandidateHeap(k);
    if(!heap)
    {
        logMessage("Failed to create heap for kNN query", ERROR);
        return;
    }

    SearchBatch* leafBatch = initSearchBatch(&query, 1);
    if(!leafBatch)
    {
        logMessage("Failed to initialize leaf search batch", ERROR);
        freeCandidateHeap(heap);
        return;
    }

    leafBatch = leafSearch(leafBatch);
    if(!leafBatch->results || !leafBatch->results[0])
    {
        logMessage("Leaf search failed or returned null leaf", ERROR);
        freeSearchBatch(leafBatch);
        freeCandidateHeap(heap);
        return;
    }

    KDNode* startLeaf = leafBatch->results[0];
    snprintf(msg, sizeof(msg), "Starting leaf found, collecting initial candidates");
    logMessage(msg, DEBUG);

    float radius = INFINITY;
    collectPointsFromLeaf(heap, startLeaf, query);
    radius = getWorstDistance(heap);

    snprintf(msg, sizeof(msg), "Initial candidates collected: count=%u, radius=%.4f", heap->size, radius == INFINITY ? INFINITY : sqrtf(radius));
    logMessage(msg, INFO);

    uint32_t nodesVisited = 1;
    KDNode* current = startLeaf;
    while(current && current->parent)
    {
        KDNode* parent = current->parent;
        if(parent->type == INTERNAL)
        {
            uint8_t dim = parent->data.internal.splitDim;
            float splitVal = parent->data.internal.splitValue;
            float distToPlane = fabs(query->coords[dim] - splitVal);
            float currentRadius = getWorstDistance(heap);
            float currentRadiusLinear = (currentRadius == INFINITY) ? INFINITY : sqrtf(currentRadius);

            snprintf(msg, sizeof(msg), "Backtrack at parent (dim=%u, split=%.4f): distToPlane=%.4f, radius=%.4f", dim, splitVal, distToPlane, currentRadiusLinear);
            logMessage(msg, DEBUG);

            if(distToPlane < currentRadiusLinear)
            {
                KDNode* otherChild = (current == parent->data.internal.left) ? parent->data.internal.right : parent->data.internal.left;
                if(otherChild)
                {
                    snprintf(msg, sizeof(msg), "Exploring far child (distToPlane=%.4f < radius=%.4f)", distToPlane, currentRadiusLinear);
                    logMessage(msg, DEBUG);

                    backtrackSearch(otherChild, query, &radius, heap);
                    nodesVisited++;
                }
            }
        }

        current = parent;
    }

    snprintf(msg, sizeof(msg), "kNN query completed: nodesVisited=%u, candidates=%u", nodesVisited, heap->size);
    logMessage(msg, INFO);

    result->count = heap->size;
    result->neighbors = calloc(heap->size, sizeof(point*));
    result->distances = calloc(heap->size, sizeof(float));

    KNNCandidate* sorted = malloc(heap->size * sizeof(KNNCandidate));
    for(uint32_t i = 0; i < heap->size; ++i)
        sorted[heap->size - 1 - i] = heapPopMax(heap);

    for(uint32_t i = 0; i < heap->size; ++i)
    {
        result->neighbors[i] = sorted[i].p;
        result->distances[i] = sqrtf(sorted[i].distance);

        snprintf(msg, sizeof(msg), "Result %u: distance=%.4f", i, result->distances[i]);
        logMessage(msg, DEBUG);
    }

    free(sorted);
    freeCandidateHeap(heap);
    freeSearchBatch(leafBatch);
}

static inline float getApproxRadius(float radius, float epsilon)
{
    return radius / (1.0f + epsilon);
}

static bool shouldPruneApprox(KDNode* node, point* query, float radius, float epsilon)
{
    if(!node)
        return true;

    if(radius >= INFINITY)
        return false;

    float approxRadius = getApproxRadius(radius, epsilon);
    float minDist = minDistToSplitPlane(node, query);

    return minDist > approxRadius;
}

static void backtrackSearchApprox(KDNode* node, point* query, float* radius, KNNCandidateHeap* heap, float epsilon)
{
    if(!node)
        return;

    *radius = getWorstDistance(heap);

    if(shouldPruneApprox(node, query, *radius, epsilon))
        return;

    if(node->type == LEAF)
    {
        collectPointsFromLeaf(heap, node, query);
        *radius = getWorstDistance(heap);
        return;
    }

    uint8_t dim = node->data.internal.splitDim;
    float splitVal = node->data.internal.splitValue;
    KDNode* nearChild = (query->coords[dim] < splitVal) ? node->data.internal.left : node->data.internal.right;
    KDNode* farChild = (query->coords[dim] < splitVal) ? node->data.internal.right : node->data.internal.left;

    backtrackSearchApprox(nearChild, query, radius, heap, epsilon);

    *radius = getWorstDistance(heap);
    float distToPlane = fabs(query->coords[dim] - splitVal);
    float approxRadius = getApproxRadius(*radius, epsilon);

    if(distToPlane < approxRadius)
        backtrackSearchApprox(farChild, query, radius, heap, epsilon);
}

static void singleApproxKNNQuery(point* query, uint32_t k, KDNode* root, KNNResult* result, float epsilon)
{
    char msg[256];

    if(!query || !root || k == 0)
    {
        logMessage("Invalid arguments to singleApproxKNNQuery", ERROR);
        return;
    }

    if(epsilon <= 0.0f)
    {
        logMessage("Invalid epsilon value for approximate kNN", ERROR);
        return;
    }

    snprintf(msg, sizeof(msg), "Starting approximate kNN query: k=%u, epsilon=%.4f", k, epsilon);
    logMessage(msg, INFO);

    KNNCandidateHeap* heap = createCandidateHeap(k);
    if(!heap)
    {
        logMessage("Failed to create heap for approximate kNN", ERROR);
        return;
    }

    SearchBatch* leafBatch = initSearchBatch(&query, 1);
    if(!leafBatch)
    {
        logMessage("Failed to initialize leaf search batch", ERROR);
        freeCandidateHeap(heap);
        return;
    }

    leafBatch = leafSearch(leafBatch);
    if(!leafBatch->results || !leafBatch->results[0])
    {
        logMessage("Leaf search failed for approximate kNN", ERROR);
        freeSearchBatch(leafBatch);
        freeCandidateHeap(heap);
        return;
    }

    KDNode* startLeaf = leafBatch->results[0];
    logMessage("Starting leaf found for approximate search", DEBUG);

    float radius = INFINITY;
    collectPointsFromLeaf(heap, startLeaf, query);
    radius = getWorstDistance(heap);

    float radiusLinear = (radius == INFINITY) ? INFINITY : sqrtf(radius);
    snprintf(msg, sizeof(msg), "Initial candidates: count=%u, radius=%.4f", heap->size, radiusLinear);
    logMessage(msg, INFO);

    uint32_t nodesVisited = 1;
    uint32_t prunedByApprox = 0;

    KDNode* current = startLeaf;
    while(current && current->parent)
    {
        KDNode* parent = current->parent;
        if(parent->type == INTERNAL)
        {
            uint8_t dim = parent->data.internal.splitDim;
            float splitVal = parent->data.internal.splitValue;
            float distToPlane = fabs(query->coords[dim] - splitVal);
            float currentRadius = getWorstDistance(heap);
            float approxRadius = (currentRadius == INFINITY) ? INFINITY : sqrtf(getApproxRadius(currentRadius, epsilon));

            snprintf(msg, sizeof(msg), "Approx backtrack: distToPlane=%.4f, approxRadius=%.4f, epsilon=%.4f", distToPlane, approxRadius, epsilon);
            logMessage(msg, DEBUG);

            if(distToPlane < approxRadius)
            {
                KDNode* otherChild = (current == parent->data.internal.left) ? parent->data.internal.right : parent->data.internal.left;
                if(otherChild)
                {
                    snprintf(msg, sizeof(msg), "Exploring far child (distToPlane=%.4f < approxRadius=%.4f)", distToPlane, approxRadius);
                    logMessage(msg, DEBUG);

                    backtrackSearchApprox(otherChild, query, &radius, heap, epsilon);
                    nodesVisited++;
                }
            }
            else
            {
                ++prunedByApprox;
                snprintf(msg, sizeof(msg), "Far child pruned (distToPlane=%.4f >= approxRadius=%.4f)", distToPlane, approxRadius);
                logMessage(msg, DEBUG);
            }
        }
        current = parent;
    }

    snprintf(msg, sizeof(msg), "Approx kNN completed: nodesVisited=%u, prunedByApprox=%u, candidates=%u", nodesVisited, prunedByApprox, heap->size);
    logMessage(msg, INFO);

    result->count = heap->size;
    result->neighbors = calloc(heap->size, sizeof(point*));
    result->distances = calloc(heap->size, sizeof(float));

    KNNCandidate* sorted = malloc(heap->size * sizeof(KNNCandidate));
    for(uint32_t i = 0; i < heap->size; ++i)
        sorted[heap->size - 1 - i] = heapPopMax(heap);

    for(uint32_t i = 0; i < heap->size; ++i)
    {
        result->neighbors[i] = sorted[i].p;
        result->distances[i] = sqrtf(sorted[i].distance);
    }

    free(sorted);
    freeCandidateHeap(heap);
    freeSearchBatch(leafBatch);
}

KNNQueryBatch* initKNNBatch(point** queries, uint32_t batchSize, uint32_t k)
{
    logMessage("Initializing kNN batch", DEBUG);

    if(!queries || batchSize == 0 || k == 0)
    {
        logMessage("Invalid arguments for kNN batch", ERROR);
        return NULL;
    }

    KNNQueryBatch* batch = malloc(sizeof(KNNQueryBatch));
    if(!batch)
    {
        logMessage("Failed to allocate kNN batch", ERROR);
        return NULL;
    }

    batch->queries = queries;
    batch->size = batchSize;
    batch->k = k;
    batch->results = calloc(batchSize, sizeof(KNNResult));

    if(!batch->results)
    {
        logMessage("Failed to allocate kNN results array", ERROR);
        free(batch);
        return NULL;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "kNN batch initialized: %u queries, k=%u", batchSize, k);
    logMessage(msg, INFO);

    return batch;
}

void freeKNNBatch(KNNQueryBatch* batch)
{
    if(!batch)
        return;

    if(batch->results)
    {
        for(uint32_t i = 0; i < batch->size; ++i)
        {
            free(batch->results[i].neighbors);
            free(batch->results[i].distances);
        }

        free(batch->results);
    }

    free(batch);
    logMessage("kNN batch freed", DEBUG);
}

KNNQueryBatch* batchKNN(KNNQueryBatch* batch)
{
    logMessage("Starting batch kNN search", DEBUG);

    if(!batch || !batch->queries || batch->size == 0)
    {
        logMessage("Invalid batch for kNN search", ERROR);
        return batch;
    }

    KDTree* tree = getData()->tree;
    if(!tree || !tree->root)
    {
        logMessage("Tree not initialized for kNN search", ERROR);
        return batch;
    }

    #pragma omp parallel for
    for(uint32_t i = 0; i < batch->size; ++i)
        singleKNNQuery(batch->queries[i], batch->k, tree->root, &batch->results[i]);

    char msg[256];
    snprintf(msg, sizeof(msg), "Batch kNN search completed for %u queries", batch->size);
    logMessage(msg, INFO);

    return batch;
}

KNNQueryBatch* batchApproximateKNN(KNNQueryBatch* batch, float epsilon)
{
    logMessage("Starting batch approximate kNN search", DEBUG);

    if (!batch || epsilon <= 0.0f)
    {
        logMessage("Invalid arguments for approximate kNN", ERROR);
        return batch;
    }

    KDTree* tree = getData()->tree;
    if (!tree || !tree->root)
    {
        logMessage("Tree not initialized for approximate kNN", ERROR);
        return batch;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "Approximate kNN with epsilon=%.4f", epsilon);
    logMessage(msg, INFO);

    #pragma omp parallel for
    for (uint32_t i = 0; i < batch->size; ++i)
        singleApproxKNNQuery(batch->queries[i], batch->k, tree->root, &batch->results[i], epsilon);

    snprintf(msg, sizeof(msg), "Batch approximate kNN completed for %u queries", batch->size);
    logMessage(msg, INFO);

    return batch;
}

#ifndef BUILD_H
#define BUILD_H

#include "kdTree/components.h"

typedef struct Bucket Bucket;

typedef struct ReplicaInfo ReplicaInfo;

KDTree* onChipBuild(point** points, size_t size);
KDGroup** logStarDecompose(KDTree* tree);
KDTree* replicate(KDTree* original, KDGroup** groups);

KDTree* buildPIMkdtree(point** points, size_t totalSize);

#endif

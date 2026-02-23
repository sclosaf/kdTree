#ifndef KDTREEBUILD_H
#define KDTREEBUILD_H

#include "kdTree/kdTree.h"

typedef struct Bucket
{
    point** bucket;
    size_t size;
} Bucket;

KDTree* onChipBuild(point** points, size_t size);
// KDGroup** logStarDecompose(KDTree* tree);
// KDTree* replicate(KDTree* tree, KDGroup** groups)

#endif

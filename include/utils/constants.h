#ifndef CONSTANTS_H
#define CONSTANTS_H

// DEFINE VARIABLE VALUES

#ifndef DIMENSIONS
#define DIMENSIONS 2
#endif

#ifndef ALPHA
#define ALPHA 2.0f
#endif

#ifndef BETA
#define BETA 2.0f
#endif

#ifndef LEAF_WRAP_THRESHOLD
#define LEAF_WRAP_THRESHOLD 32
#endif

#ifndef OVERSAMPLING_RATE
#define OVERSAMPLING_RATE 32
#endif

#ifndef SKETCH_HEIGHT
#define SKETCH_HEIGHT 6
#endif

#ifndef CHUNK_SIZE
#define CHUNK_SIZE (1 << SKETCH_HEIGHT)
#endif

#endif

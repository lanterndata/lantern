#include "postgres.h"

#include "distfunc.h"

#include "math.h"

extern dist_t l2_dist_impl(coord_t const* ax, coord_t const* bx, size_t dim)
{
    dist_t distance = 0.0;
    for(size_t i = 0; i < dim; i++) {
        dist_t diff = ax[ i ] - bx[ i ];
        distance += diff * diff;
    }
    return sqrtf(distance);
}

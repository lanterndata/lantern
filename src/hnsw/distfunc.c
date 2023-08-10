#include "postgres.h"

#include "distfunc.h"

#include "math.h"

#include "utils.h"

extern float l2sq_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    float4 distance = 0.0;
    for(size_t i = 0; i < dim; i++) {
        float4 diff = ax[ i ] - bx[ i ];
        distance += diff * diff;
    }
    return distance;
}

extern float cosine_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    float4 dot = 0.0;
    float4 mag_a = 0.0;
    float4 mag_b = 0.0;
    for (size_t i = 0; i < dim; i++) {
        dot += ax[ i ] * bx[ i ];
        mag_a += ax [ i ] * ax[ i ];
        mag_b += bx [ i ] * bx[ i ];
    }
    return dot / sqrtf(mag_a * mag_b);
}

extern float l1_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    float4 distance = 0.0;
    for (size_t i =0; i < dim; i++) {
        distance += fabs(ax[ i ] - bx[ i ]);
    }
    return distance;
}

extern float l2_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    return sqrtf(l2sq_dist_impl(ax, bx, dim));
}

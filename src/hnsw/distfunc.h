#ifndef LDB_HNSW_DISTFUNC_H
#define LDB_HNSW_DISTFUNC_H
#include <catalog/pg_type.h>

extern float4 l2sq_dist_impl(float4 const* ax, float4 const* bx, float4 dim);
extern float4 cosine_dist_impl(float4 const* ax, float4 const* bx, float4 dim);
extern float4 l1_dist_impl(float4 const* ax, float4 const* bx, float4 dim);
extern float4 l2_dist_impl(float4 const* ax, float4 const* bx, float4 dim);
#endif  // LDB_HNSW_DISTFUNC_H

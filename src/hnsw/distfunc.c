#ifdef __linux__
#include <immintrin.h>
#include <cpuid.h>
#endif

#include "postgres.h"

#include "distfunc.h"

#include "math.h"

#include "utils.h"

unsigned int AVX2_PRESENT = 0;

#ifdef __linux__
unsigned int CheckAVX(){
    unsigned int eax, ebx, ecx, edx;
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);

    // needed to preserve fp registers
    if (!(ecx & bit_OSXSAVE) || !(ecx & bit_AVX)) {
        return 0;
    }

    __get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx);
    return (ebx & bit_AVX2) != 0;
}

__attribute__((constructor)) void InitLanternUtils()
{
    AVX2_PRESENT = CheckAVX();
}
#if defined(__AVX2__)
static inline float SumAVXReg(float arr[8])
{
    return arr [ 0 ] + arr[ 1 ] + arr[ 2 ] + arr[ 3 ] + arr[ 4 ] + arr[ 5 ] + arr[ 6 ] + arr[ 7 ];
}
#endif
#endif

extern float l2sq_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    float4 distance = 0.0;
    if (AVX2_PRESENT && dim > 7) {
#if defined(__AVX2__) && defined(__linux__)
        __m256 acc = _mm256_setzero_ps();
        size_t i;
        for(i = 0; i+7 < dim; i += 8) {
            __m256 as = _mm256_loadu_ps(ax + i);
            __m256 bs = _mm256_loadu_ps(bx + i);
            __m256 diff = _mm256_sub_ps(as, bs);
            acc = _mm256_add_ps(acc, _mm256_mul_ps(diff, diff));
        }

        float result[8];
        _mm256_storeu_ps(result, acc);
        distance = SumAVXReg(result);

        for (; i < dim; ++i) {
            float4 diff = ax[ i ] - bx[ i ];
            distance += diff * diff;
        }
#endif
    }
    else {
        for(size_t i = 0; i < dim; i++) {
            float4 diff = ax[ i ] - bx[ i ];
            distance += diff * diff;
        }
    }
    return distance;
}

extern float cosine_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    float distance = 0.0;
    if (AVX2_PRESENT && dim > 7){
#if defined(__AVX2__) && defined(__linux__)
        __m256 dot = _mm256_setzero_ps();
        __m256 mag_a = _mm256_setzero_ps();
        __m256 mag_b = _mm256_setzero_ps();
        size_t i;
        for(i = 0; i+7 < dim; i += 8) {
            __m256 as = _mm256_loadu_ps(ax + i);
            __m256 bs = _mm256_loadu_ps(bx + i);
            dot = _mm256_add_ps(dot, _mm256_mul_ps(as, bs));
            mag_a = _mm256_add_ps(mag_a, _mm256_mul_ps(as, as));
            mag_b = _mm256_add_ps(mag_b, _mm256_mul_ps(bs, bs));
        }

        float mag_a_res[8];
        float mag_b_res[8];
        float dot_res[8];

        _mm256_storeu_ps(mag_a_res, mag_a);
        _mm256_storeu_ps(mag_b_res, mag_b);
        _mm256_storeu_ps(dot_res, dot);

        float dotf = SumAVXReg(dot_res);
        float mag_af = SumAVXReg(mag_a_res);
        float mag_bf = SumAVXReg(mag_b_res);

        for (; i < dim; ++i) {
            dotf += ax[ i ] * bx[ i ];
            mag_af += ax [ i ] * ax[ i ];
            mag_bf += bx [ i ] * bx[ i ];
        }
        distance = dotf / sqrtf(mag_af * mag_bf);
#endif
    }
    else {
        float4 dot = 0.0;
        float4 mag_a = 0.0;
        float4 mag_b = 0.0;
        for (size_t i = 0; i < dim; i++) {
            dot += ax[ i ] * bx[ i ];
            mag_a += ax [ i ] * ax[ i ];
            mag_b += bx [ i ] * bx[ i ];
        }
        distance =  dot / sqrtf(mag_a * mag_b);
    }
    return distance;
}

extern float l1_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    float4 distance = 0.0;
    if (AVX2_PRESENT && dim > 7){
#if defined(__AVX2__) && defined(__linux__)
        __m256 acc = _mm256_setzero_ps();
        __m256 mask = _mm256_castsi256_ps(_mm256_set1_epi32(0x7FFFFFFF));
        size_t i;
        for(i = 0; i+7 < dim; i += 8) {
            __m256 as = _mm256_loadu_ps(ax + i);
            __m256 bs = _mm256_loadu_ps(bx + i);
            __m256 diff = _mm256_sub_ps(as, bs);
            acc = _mm256_add_ps(acc, _mm256_and_ps(diff, mask));
        }

        float result[8];
        _mm256_storeu_ps(result, acc);
        distance = SumAVXReg(result);

        for (; i < dim; ++i) {
            distance += fabs(ax[ i ] - bx[ i ]);
        }
#endif
    }
    else {
        for (size_t i =0; i < dim; i++) {
            distance += fabs(ax[ i ] - bx[ i ]);
        }

    }
    return distance;
}

extern float l2_dist_impl(float4 const* ax, float4 const* bx, float4 dim)
{
    return sqrtf(l2sq_dist_impl(ax, bx, dim));
}

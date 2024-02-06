#ifndef LDB_BENCH_H
#define LDB_BENCH_H
#ifdef LANTERN_BENCH
#include <stdint.h>
#include <time.h>
#endif

#ifdef LANTERN_BENCH

void bench_save(const char* name, uint64_t micros);

#define LanternBench(name, code)                                                                   \
    do {                                                                                           \
        struct timespec start, end;                                                                \
        uint64_t        micros;                                                                    \
        clock_gettime(CLOCK_MONOTONIC, &start);                                                    \
        (code);                                                                                    \
        clock_gettime(CLOCK_MONOTONIC, &end);                                                      \
        micros = (end.tv_sec - start.tv_sec) * 1000000LL + (end.tv_nsec - start.tv_nsec) / 1000LL; \
        bench_save(name, micros);                                                                  \
    } while(0)
#else
#define LanternBench(name, code) (code)
#endif
#endif  // LDB_BENCH_H

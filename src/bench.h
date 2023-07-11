#ifndef LDB_BENCH_H
#define LDB_BENCH_H
#ifdef LANTERN_BENCH
#include "portability/instr_time.h"
#endif

#ifdef LANTERN_BENCH
#define LanternBench(name, code)                                            \
    do {                                                                    \
        instr_time start;                                                   \
        instr_time duration;                                                \
        INSTR_TIME_SET_CURRENT(start);                                      \
        (code);                                                             \
        INSTR_TIME_SET_CURRENT(duration);                                   \
        INSTR_TIME_SUBTRACT(duration, start);                               \
        elog(INFO, "%s: %.3f ms", name, INSTR_TIME_GET_MILLISEC(duration)); \
    } while(0)
#else
#define LanternBench(name, code) (code)
#endif
#endif  // LDB_BENCH_H
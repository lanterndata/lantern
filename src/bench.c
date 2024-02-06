#ifdef LANTERN_BENCH
// #if 1
#include <postgres.h>

#include "bench.h"

#include <string.h>

static const char* name_list[ 100 ];
static float8      sum[ 100 ];
static int64_t     count[ 100 ];

void bench_save(const char* name, float millis)
{
    bool      found = false;
    const int arr_len = sizeof(sum) / sizeof(sum[ 0 ]);
    int       i;
    for(i = 0; i < arr_len; i++) {
        if(name_list[ i ] == NULL) break;
        if(strcmp(name, name_list[ i ]) == 0) {
            found = true;
            sum[ i ] += millis;
            count[ i ]++;
            if(count[ i ] % 1000 == 0) {
                elog(INFO, "BENCH: %s: %.3fms", name, sum[ i ] / count[ i ]);
            }
        }
    }
    if(!found) {
        if(i >= arr_len) {
            elog(WARNING, "Ran out of space to log bench for %s", name);
            return;
        }
        name_list[ i ] = name;
        sum[ i ] = millis;
        count[ i ] = 0;
    }
}

#endif

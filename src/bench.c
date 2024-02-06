#ifdef LANTERN_BENCH
// #if 1
#include <postgres.h>

#include "bench.h"

#include <string.h>
#include <time.h>

static const char* name_list[ 100 ];
static float8      sum[ 100 ];
static int64_t     count[ 100 ];
static time_t      last_log_time;

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

    // print summary periodically
    time_t t = time(0);
    if(difftime(t, last_log_time) > 5) {
        last_log_time = t;
        for(int j = 0; j < arr_len; j++) {
            if(name_list[ j ] == NULL) break;
            elog(INFO, "BENCH: %s: count: %ld avg: %.3fms", name_list[ j ], count[ j ], sum[ j ] / count[ j ]);
        }
        elog(INFO, "\n\n");
    }
}

#endif

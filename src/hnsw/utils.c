#include <postgres.h>

#include "utils.h"

#include <regex.h>
#include <string.h>

#include "hnsw.h"
#include "options.h"
#include "usearch.h"

void LogUsearchOptions(usearch_init_options_t *opts)
{
    /*todo:: in usearch.h create const char arrays like
char* scalar_names = {
    usearch_scalar_f32_k: "f32",
    usearch_scalar_f64_k: "f64"
}
so below the human readable string names can be printed
*/
    elog(INFO,
         "usearch_init_options_t: metric_kind: %d, metric: %p, "
         "quantization: %d, dimensions: %ld, connectivity: %ld, "
         "expansion_add: %ld, expansion_search: %ld",
         opts->metric_kind,
         opts->metric,
         opts->quantization,
         opts->dimensions,
         opts->connectivity,
         opts->expansion_add,
         opts->expansion_search);
}

void PopulateUsearchOpts(Relation index, usearch_init_options_t *opts)
{
    opts->connectivity = HnswGetM(index);
    opts->expansion_add = HnswGetEfConstruction(index);
    opts->expansion_search = HnswGetEf(index);
    opts->metric_kind = HnswGetMetricKind(index);
    opts->metric = NULL;
    opts->quantization = usearch_scalar_f32_k;
}

int IsInsideQuotes(const char *query, int op_start, int op_end)
{
    if(op_start == 0) return 0;

    int    single_quote_start = -1;
    size_t offset = 0;

    while(strcmp(query + offset, "\0") != 0) {
        char s = *(query + offset);
        if(s == '\'') {
            // if current char is single quote
            if(single_quote_start == -1) {
                // if we didn't encounter any unclosed single quote before
                // keep the start offset
                single_quote_start = offset;
            } else {
                // if we already have open single quote
                // check if our operator is inside quotes
                // return 1
                if(single_quote_start < op_start && offset > op_end) {
                    return 1;
                }
                single_quote_start = -1;
            }
        }
        offset += 1;
    }

    return 0;
}

int CheckOperatorUsage(const char *query)
{
    const char *pattern = "(<->)";
    const char *orderby_pattern = "order by";
    regex_t     regex;
    regex_t     regex2;
    regmatch_t  matches[ 1 ];
    int         reti;
    int         reti2;
    int         status = 0;

    reti = regcomp(&regex, pattern, REG_EXTENDED | REG_ICASE);
    reti2 = regcomp(&regex2, orderby_pattern, REG_EXTENDED | REG_ICASE);
    if(reti || reti2) {
        elog(ERROR, "Could not compile regex");
        return status;
    }

    // Find all occurrences of the <-> operator
    int offset = 0;
    while((reti = regexec(&regex, query + offset, 1, matches, 0)) == 0) {
        long start = offset + matches[ 0 ].rm_so;
        long end = offset + matches[ 0 ].rm_eo;
        char substring[ start - offset ];
        strncpy(substring, query + offset, start - offset);
        // check if there is an ORDER BY
        // in the latest matched substring
        if(regexec(&regex2, substring, 0, NULL, 0) && !IsInsideQuotes(query, start, end)) {
            status = 1;
            break;
        }
        offset += matches[ 0 ].rm_eo;
    }

    regfree(&regex);
    regfree(&regex2);
    return status;
}

#ifndef LDB_HOOKS_POST_PARSE_H
#define LDB_HOOKS_POST_PARSE_H

#include <postgres.h>

#include <nodes/pg_list.h>
#include <parser/analyze.h>

extern post_parse_analyze_hook_type original_post_parse_analyze_hook;
void                                post_parse_analyze_hook_with_operator_check(ParseState *pstate,
                                                                                Query      *query
#if PG_VERSION_NUM >= 140000
                                                 ,
                                                 JumbleState *jstate
#endif
);

#endif  // LDB_HOOKS_POST_PARSE_H
#include <postgres.h>

#include "post_parse.h"

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/analyze.h>
#include <parser/parse_oper.h>
#include <utils/catcache.h>
#include <utils/guc.h>

#include "utils.h"

post_parse_analyze_hook_type original_post_parse_analyze_hook = NULL;

typedef struct
{
    List *oidList;
} OperatorUsedContext;

static bool operator_used_walker(Node *node, OperatorUsedContext *context)
{
    if(node == NULL) return false;
    if(IsA(node, Query)) return query_tree_walker((Query *)node, operator_used_walker, (void *)context, 0);
    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context->oidList, opExpr->opno)) {
            return true;
        }
    }
    return expression_tree_walker(node, operator_used_walker, (void *)context);
}

static bool is_operator_used(Node *node, List *oidList)
{
    OperatorUsedContext context;
    context.oidList = oidList;
    return operator_used_walker(node, &context);
}

typedef struct
{
    List *sortGroupRefs;
} SortGroupRefContext;

static bool sort_group_ref_walker(Node *node, SortGroupRefContext *context)
{
    if(node == NULL) return false;
    if(IsA(node, Query)) {
        Query    *query = (Query *)node;
        ListCell *lc;
        foreach(lc, query->sortClause) {
            SortGroupClause *sortGroupClause = (SortGroupClause *)lfirst(lc);
            context->sortGroupRefs = lappend_int(context->sortGroupRefs, sortGroupClause->tleSortGroupRef);
        }
        return query_tree_walker((Query *)node, sort_group_ref_walker, (void *)context, 0);
    }
    return expression_tree_walker(node, sort_group_ref_walker, (void *)context);
}

static List *get_sort_group_refs(Node *node)
{
    SortGroupRefContext context;
    context.sortGroupRefs = NIL;
    sort_group_ref_walker(node, &context);
    return context.sortGroupRefs;
}

typedef struct
{
    List *oidList;
    List *sortGroupRefs;
    bool  usedCorrectly;
} OperatorUsedCorrectlyContext;

static bool operator_used_correctly_walker(Node *node, OperatorUsedCorrectlyContext *context)
{
    if(node == NULL) return false;
    if(IsA(node, Query)) return query_tree_walker((Query *)node, operator_used_correctly_walker, (void *)context, 0);
    if(IsA(node, TargetEntry)) {
        TargetEntry *te = (TargetEntry *)node;
        if(te->resjunk && list_member_int(context->sortGroupRefs, te->ressortgroupref)) {
            if(IsA(te->expr, OpExpr)) {
                OpExpr *opExpr = (OpExpr *)te->expr;
                if(list_member_oid(context->oidList, opExpr->opno)) {
                    Node *arg1 = (Node *)linitial(opExpr->args);
                    Node *arg2 = (Node *)lsecond(opExpr->args);
                    bool  isVar1 = IsA(arg1, Var);
                    bool  isVar2 = IsA(arg2, Var);
                    if(isVar1 && isVar2) {
                        return false;
                    } else if(!isVar1 && !isVar2) {
                        return true;
                    } else if(isVar1) {
                        return operator_used_correctly_walker(arg2, context);
                    } else {
                        return operator_used_correctly_walker(arg1, context);
                    }
                }
            }
        }
    }
    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context->oidList, opExpr->opno)) {
            return true;
        }
    }

    return expression_tree_walker(node, operator_used_correctly_walker, (void *)context);
}

static bool is_operator_used_correctly(Node *node, List *oidList, List *sortGroupRefs)
{
    OperatorUsedCorrectlyContext context;
    context.oidList = oidList;
    context.sortGroupRefs = sortGroupRefs;
    return !operator_used_correctly_walker(node, &context);
}

void post_parse_analyze_hook_with_operator_check(ParseState *pstate,
                                                 Query      *query
#if PG_VERSION_NUM >= 140000
                                                 ,
                                                 JumbleState *jstate
#endif
)
{
    if(original_post_parse_analyze_hook) {
#if PG_VERSION_NUM >= 140000
        original_post_parse_analyze_hook(pstate, query, jstate);
#else
        original_post_parse_analyze_hook(pstate, query);
#endif
    }

    List *oidList = ldb_get_operator_oids();
    if(is_operator_used((Node *)query, oidList)) {
        List *sort_group_refs = get_sort_group_refs(query);
        if(!is_operator_used_correctly(query, oidList, sort_group_refs)) {
            elog(ERROR, "Operator <-> has no standalone meaning and is reserved for use in vector index lookups only");
        }
        list_free(sort_group_refs);
    }
    list_free(oidList);
}
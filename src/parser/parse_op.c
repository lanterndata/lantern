#include <postgres.h>

#include "parse_op.h"

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <parser/parse_oper.h>
#include <utils/catcache.h>
#include <utils/guc.h>

static bool is_operator_used_correctly(Node *node, List *oidList, List *sortGroupRefs)
{
    if(node == NULL) return false;

    if(IsA(node, TargetEntry)) {
        TargetEntry *te = (TargetEntry *)node;
        if(te->resjunk && list_member_int(sortGroupRefs, te->ressortgroupref)) {
            if(IsA(te->expr, OpExpr)) {
                OpExpr *opExpr = (OpExpr *)te->expr;
                if(list_member_oid(oidList, opExpr->opno)) {
                    Node *arg1 = (Node *)linitial(opExpr->args);
                    Node *arg2 = (Node *)lsecond(opExpr->args);
                    bool  isVar1 = IsA(arg1, Var);
                    bool  isVar2 = IsA(arg2, Var);
                    if(isVar1 && isVar2) {
                        return false;
                    } else if(!isVar1 && !isVar2) {
                        return true;
                    } else if(isVar1) {
                        return is_operator_used_correctly(arg2, oidList, sortGroupRefs);
                    } else if(isVar2) {
                        return is_operator_used_correctly(arg1, oidList, sortGroupRefs);
                    }
                }
            }
        }
        if(is_operator_used_correctly((Node *)te->expr, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, FuncExpr)) {
        FuncExpr *funcExpr = (FuncExpr *)node;
        if(is_operator_used_correctly(funcExpr->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, Query)) {
        ListCell *lc;
        Query    *query = (Query *)node;
        if(is_operator_used_correctly(query->returningList, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)query->targetList, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)query->jointree, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)query->rtable, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)query->cteList, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, CommonTableExpr)) {
        CommonTableExpr *cte = (CommonTableExpr *)node;
        if(is_operator_used_correctly(cte->ctequery, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *)node;
        if(rte->rtekind == RTE_SUBQUERY) {
            if(is_operator_used_correctly((Node *)rte->subquery, oidList, sortGroupRefs)) {
                return true;
            }
        }
    }

    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(oidList, opExpr->opno) || is_operator_used_correctly(opExpr->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, List)) {
        List     *list = (List *)node;
        ListCell *lc;
        foreach(lc, list) {
            if(is_operator_used_correctly((Node *)lfirst(lc), oidList, sortGroupRefs)) {
                return true;
            }
        }
        return false;
    }

    if(IsA(node, ArrayExpr)) {
        ArrayExpr *arrayExpr = (ArrayExpr *)node;
        if(is_operator_used_correctly(arrayExpr->elements, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, SubLink)) {
        SubLink *sublink = (SubLink *)node;
        if(is_operator_used_correctly(sublink->subselect, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, CoalesceExpr)) {
        CoalesceExpr *coalesce = (CoalesceExpr *)node;
        if(is_operator_used_correctly(coalesce->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, Aggref)) {
        Aggref *aggref = (Aggref *)node;
        if(is_operator_used_correctly(aggref->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, FromExpr)) {
        FromExpr *fromExpr = (FromExpr *)node;
        if(is_operator_used_correctly(fromExpr->fromlist, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)fromExpr->quals, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, JoinExpr)) {
        JoinExpr *joinExpr = (JoinExpr *)node;
        if(is_operator_used_correctly((Node *)joinExpr->larg, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)joinExpr->rarg, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)joinExpr->quals, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, CaseExpr)) {
        CaseExpr *caseExpr = (CaseExpr *)node;
        if(is_operator_used_correctly(caseExpr->args, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)caseExpr->defresult, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, CaseWhen)) {
        CaseWhen *caseWhen = (CaseWhen *)node;
        if(is_operator_used_correctly((Node *)caseWhen->expr, oidList, sortGroupRefs)
           || is_operator_used_correctly((Node *)caseWhen->result, oidList, sortGroupRefs)) {
            return true;
        }
    }

    return false;
}

List *get_sort_group_refs(Node *node, List *sort_group_refs)
{
    List *new_sort_group_refs = sort_group_refs;

    if(node == NULL) return new_sort_group_refs;

    if(IsA(node, TargetEntry)) {
        TargetEntry *te = (TargetEntry *)node;
        new_sort_group_refs = get_sort_group_refs(te->expr, new_sort_group_refs);
    }

    if(IsA(node, FuncExpr)) {
        FuncExpr *funcExpr = (FuncExpr *)node;
        new_sort_group_refs = get_sort_group_refs(funcExpr->args, new_sort_group_refs);
    }

    if(IsA(node, Query)) {
        Query    *query = (Query *)node;
        ListCell *lc;
        foreach(lc, query->sortClause) {
            SortGroupClause *sortGroupClause = (SortGroupClause *)lfirst(lc);
            new_sort_group_refs = lappend_int(new_sort_group_refs, sortGroupClause->tleSortGroupRef);
        }
        new_sort_group_refs = get_sort_group_refs(query->returningList, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(query->targetList, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(query->jointree, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(query->rtable, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(query->rtable, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(query->cteList, new_sort_group_refs);
    }

    if(IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *)node;
        if(rte->rtekind == RTE_SUBQUERY) {
            new_sort_group_refs = get_sort_group_refs(rte->subquery, new_sort_group_refs);
        }
    }

    if(IsA(node, CommonTableExpr)) {
        CommonTableExpr *cte = (CommonTableExpr *)node;
        new_sort_group_refs = get_sort_group_refs(cte->ctequery, new_sort_group_refs);
    }

    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        new_sort_group_refs = get_sort_group_refs(opExpr->args, new_sort_group_refs);
    }

    if(IsA(node, List)) {
        List     *list = (List *)node;
        ListCell *lc;
        foreach(lc, list) {
            new_sort_group_refs = get_sort_group_refs((Node *)lfirst(lc), sort_group_refs);
        }
    }

    if(IsA(node, ArrayExpr)) {
        ArrayExpr *arrayExpr = (ArrayExpr *)node;
        new_sort_group_refs = get_sort_group_refs(arrayExpr->elements, new_sort_group_refs);
    }

    if(IsA(node, SubLink)) {
        SubLink *sublink = (SubLink *)node;
        new_sort_group_refs = get_sort_group_refs(sublink->subselect, new_sort_group_refs);
    }

    if(IsA(node, CoalesceExpr)) {
        CoalesceExpr *coalesce = (CoalesceExpr *)node;
        new_sort_group_refs = get_sort_group_refs(coalesce->args, new_sort_group_refs);
    }

    if(IsA(node, Aggref)) {
        Aggref *aggref = (Aggref *)node;
        new_sort_group_refs = get_sort_group_refs(aggref->args, new_sort_group_refs);
    }

    if(IsA(node, FromExpr)) {
        FromExpr *fromExpr = (FromExpr *)node;
        new_sort_group_refs = get_sort_group_refs(fromExpr->fromlist, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(fromExpr->quals, new_sort_group_refs);
    }

    if(IsA(node, JoinExpr)) {
        JoinExpr *joinExpr = (JoinExpr *)node;
        new_sort_group_refs = get_sort_group_refs(joinExpr->larg, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(joinExpr->rarg, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(joinExpr->quals, new_sort_group_refs);
    }

    if(IsA(node, CaseWhen)) {
        CaseWhen *caseWhen = (CaseWhen *)node;
        new_sort_group_refs = get_sort_group_refs(caseWhen->expr, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(caseWhen->result, new_sort_group_refs);
    }

    if(IsA(node, CaseExpr)) {
        CaseExpr *caseExpr = (CaseExpr *)node;
        new_sort_group_refs = get_sort_group_refs(caseExpr->args, new_sort_group_refs);
        new_sort_group_refs = get_sort_group_refs(caseExpr->defresult, new_sort_group_refs);
    }

    return new_sort_group_refs;
}

bool validate_operator_usage(Node *node, List *oidList)
{
    List *sort_group_refs = get_sort_group_refs((Query *)node, NIL);

    // Check for invalid operator usage
    return is_operator_used_correctly(node, oidList, sort_group_refs);

    // Check for sort by without index
}

List *get_operator_oids(ParseState *pstate)
{
    List *oidList = NIL;

    Oid intArrayOid = INT4ARRAYOID;
    Oid floatArrayOid = FLOAT4ARRAYOID;

    List *nameList = lappend(NIL, makeString("<->"));

    Oid intOperator = LookupOperName(pstate, nameList, intArrayOid, intArrayOid, true, -1);
    Oid floatOperator = LookupOperName(pstate, nameList, floatArrayOid, floatArrayOid, true, -1);

    if(OidIsValid(intOperator)) {
        oidList = lappend_oid(oidList, intOperator);
    }
    if(OidIsValid(floatOperator)) {
        oidList = lappend_oid(oidList, floatOperator);
    }

    list_free(nameList);

    return oidList;
}
#include <postgres.h>

#include "parse_op.h"

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <parser/parse_oper.h>
#include <utils/catcache.h>
#include <utils/guc.h>

static bool checkNodeList(List *list, List *oidList, List *sortGroupRefs)
{
    ListCell *lc;
    foreach(lc, list) {
        if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList, sortGroupRefs)) {
            return true;
        }
    }
    return false;
}

bool isOperatorUsedOutsideOrderBy(Node *node, List *oidList, List *sortGroupRefs)
{
    if(node == NULL) return false;

    if(IsA(node, TargetEntry)) {
        TargetEntry *te = (TargetEntry *)node;
        if(te->resjunk && list_member_int(sortGroupRefs, te->ressortgroupref)) {
            if(IsA(te->expr, OpExpr)) {
                OpExpr *opExpr = (OpExpr *)te->expr;
                if(list_member_oid(oidList, opExpr->opno)) {
                    Node *firstArg = (Node *)linitial(opExpr->args);
                    Node *secondArg = (Node *)lsecond(opExpr->args);
                    if(IsA(firstArg, Var) || IsA(secondArg, Var)) {
                        return false;
                    }
                }
            }
        }
        if(isOperatorUsedOutsideOrderBy((Node *)te->expr, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, FuncExpr)) {
        FuncExpr *funcExpr = (FuncExpr *)node;
        if(checkNodeList(funcExpr->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, Query)) {
        ListCell *lc;
        Query    *query = (Query *)node;
        List     *localSortGroupRefs = NIL;
        foreach(lc, query->sortClause) {
            SortGroupClause *sortGroupClause = (SortGroupClause *)lfirst(lc);
            localSortGroupRefs = lappend_int(localSortGroupRefs, sortGroupClause->tleSortGroupRef);
        }
        if(checkNodeList(query->returningList, oidList, sortGroupRefs)) {
            return true;
        }
        if(checkNodeList(query->returningList, oidList, sortGroupRefs)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)query->targetList, oidList, localSortGroupRefs)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)query->jointree, oidList, localSortGroupRefs)) {
            return true;
        }
        foreach(lc, query->rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
            if(rte->rtekind == RTE_SUBQUERY) {
                if(isOperatorUsedOutsideOrderBy((Node *)rte->subquery, oidList, localSortGroupRefs)) {
                    return true;
                }
            }
        }
        foreach(lc, query->cteList) {
            CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
            if(isOperatorUsedOutsideOrderBy(cte->ctequery, oidList, localSortGroupRefs)) {
                return true;
            }
        }
    }

    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(oidList, opExpr->opno)) {
            return true;
        }
        if(checkNodeList(opExpr->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, List)) {
        List *list = (List *)node;
        if(checkNodeList(list, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, ArrayExpr)) {
        ArrayExpr *arrayExpr = (ArrayExpr *)node;
        if(checkNodeList(arrayExpr->elements, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, SubLink)) {
        SubLink *sublink = (SubLink *)node;
        if(isOperatorUsedOutsideOrderBy(sublink->subselect, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, CoalesceExpr)) {
        CoalesceExpr *coalesce = (CoalesceExpr *)node;
        if(checkNodeList(coalesce->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, Aggref)) {
        Aggref *aggref = (Aggref *)node;
        if(checkNodeList(aggref->args, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, FromExpr)) {
        FromExpr *fromExpr = (FromExpr *)node;
        if(checkNodeList(fromExpr->fromlist, oidList, sortGroupRefs)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)fromExpr->quals, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, JoinExpr)) {
        JoinExpr *joinExpr = (JoinExpr *)node;
        if(isOperatorUsedOutsideOrderBy((Node *)joinExpr->larg, oidList, sortGroupRefs)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)joinExpr->rarg, oidList, sortGroupRefs)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)joinExpr->quals, oidList, sortGroupRefs)) {
            return true;
        }
    }

    if(IsA(node, CaseExpr)) {
        CaseExpr *caseExpr = (CaseExpr *)node;
        ListCell *lc;
        foreach(lc, caseExpr->args) {
            CaseWhen *when = (CaseWhen *)lfirst(lc);
            if(isOperatorUsedOutsideOrderBy((Node *)when->expr, oidList, sortGroupRefs)) {
                return true;
            }
            if(isOperatorUsedOutsideOrderBy((Node *)when->result, oidList, sortGroupRefs)) {
                return true;
            }
        }
        if(isOperatorUsedOutsideOrderBy((Node *)caseExpr->defresult, oidList, sortGroupRefs)) {
            return true;
        }
    }

    return false;
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
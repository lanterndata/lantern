#include <postgres.h>

#include "parse_op.h"

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <parser/parse_oper.h>
#include <utils/catcache.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>

bool isOperatorUsedOutsideOrderBy(Node *node, List *oidList)
{
    if(node == NULL) return false;

    if(IsA(node, TargetEntry)) {
        TargetEntry *te = (TargetEntry *)node;
        if(te->resjunk) {
            return false;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)te->expr, oidList)) {
            return true;
        }
    }

    if(IsA(node, FuncExpr)) {
        FuncExpr *funcExpr = (FuncExpr *)node;
        ListCell *lc;
        foreach(lc, funcExpr->args) {
            if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
                return true;
            }
        }
    }

    // If it's a Query node, handle its main parts and recurse into its subqueries
    if(IsA(node, Query)) {
        Query *query = (Query *)node;

        if(isOperatorUsedOutsideOrderBy((Node *)query->targetList, oidList)) {
            return true;
        }

        if(isOperatorUsedOutsideOrderBy((Node *)query->jointree, oidList)) {
            return true;
        }

        // Recurse into RTEs that are subqueries
        ListCell *lc;
        foreach(lc, query->rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
            if(rte->rtekind == RTE_SUBQUERY) {
                if(isOperatorUsedOutsideOrderBy((Node *)rte->subquery, oidList)) {
                    return true;
                }
            }
        }

        foreach(lc, query->cteList) {
            CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
            if(isOperatorUsedOutsideOrderBy(cte->ctequery, oidList)) {
                return true;
            }
        }
    }

    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(oidList, opExpr->opno)) {
            return true;
        }
        ListCell *lc;
        foreach(lc, opExpr->args) {
            if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
                return true;
            }
        }
    }

    // If it's a list, recurse into its items
    if(IsA(node, List)) {
        List *list = (List *)node;

        ListCell *lc;
        foreach(lc, list) {
            if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
                return true;
            }
        }
    }

    if(IsA(node, ArrayExpr)) {
        ArrayExpr *arrayExpr = (ArrayExpr *)node;
        ListCell  *lc;
        foreach(lc, arrayExpr->elements) {
            if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
                return true;
            }
        }
    }

    if(IsA(node, SubLink)) {
        SubLink *sublink = (SubLink *)node;
        if(isOperatorUsedOutsideOrderBy(sublink->subselect, oidList)) {
            return true;
        }
    }

    if(IsA(node, CoalesceExpr)) {
        CoalesceExpr *coalesce = (CoalesceExpr *)node;
        ListCell     *lc;
        foreach(lc, coalesce->args) {
            if(isOperatorUsedOutsideOrderBy(lfirst(lc), oidList)) {
                return true;
            }
        }
    }

    if(IsA(node, Aggref)) {
        Aggref   *aggref = (Aggref *)node;
        ListCell *lc;
        foreach(lc, aggref->args) {
            if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
                return true;
            }
        }
    }

    if(IsA(node, FromExpr)) {
        FromExpr *fromExpr = (FromExpr *)node;
        ListCell *lc;
        foreach(lc, fromExpr->fromlist) {
            if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
                return true;
            }
        }
        if(isOperatorUsedOutsideOrderBy((Node *)fromExpr->quals, oidList)) {
            return true;
        }
    }

    if(IsA(node, JoinExpr)) {
        JoinExpr *joinExpr = (JoinExpr *)node;
        if(isOperatorUsedOutsideOrderBy((Node *)joinExpr->larg, oidList)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)joinExpr->rarg, oidList)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)joinExpr->quals, oidList)) {
            return true;
        }
    }

    return false;
}

List *get_operator_oids(ParseState *pstate)
{
    List *oidList = NIL;

    Oid intArrayOid = get_array_type(INT4OID);
    Oid floatArrayOid = get_array_type(FLOAT4OID);

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
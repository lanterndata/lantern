#include <postgres.h>

#include "parse_op.h"

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <parser/parse_oper.h>
#include <utils/catcache.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>

static bool checkNodeList(List *list, List *oidList)
{
    ListCell *lc;
    foreach(lc, list) {
        if(isOperatorUsedOutsideOrderBy((Node *)lfirst(lc), oidList)) {
            return true;
        }
    }
    return false;
}

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
        if(checkNodeList(funcExpr->args, oidList)) {
            return true;
        }
    }

    if(IsA(node, Query)) {
        ListCell *lc;
        Query    *query = (Query *)node;
        if(checkNodeList(query->returningList, oidList)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)query->targetList, oidList)) {
            return true;
        }
        if(isOperatorUsedOutsideOrderBy((Node *)query->jointree, oidList)) {
            return true;
        }
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
        if(checkNodeList(opExpr->args, oidList)) {
            return true;
        }
    }

    if(IsA(node, List)) {
        List *list = (List *)node;
        if(checkNodeList(list, oidList)) {
            return true;
        }
    }

    if(IsA(node, ArrayExpr)) {
        ArrayExpr *arrayExpr = (ArrayExpr *)node;
        if(checkNodeList(arrayExpr->elements, oidList)) {
            return true;
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
        if(checkNodeList(coalesce->args, oidList)) {
            return true;
        }
    }

    if(IsA(node, Aggref)) {
        Aggref *aggref = (Aggref *)node;
        if(checkNodeList(aggref->args, oidList)) {
            return true;
        }
    }

    if(IsA(node, FromExpr)) {
        FromExpr *fromExpr = (FromExpr *)node;
        if(checkNodeList(fromExpr->fromlist, oidList)) {
            return true;
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

    if(IsA(node, CaseExpr)) {
        CaseExpr *caseExpr = (CaseExpr *)node;
        ListCell *lc;
        foreach(lc, caseExpr->args) {
            CaseWhen *when = (CaseWhen *)lfirst(lc);
            if(isOperatorUsedOutsideOrderBy((Node *)when->expr, oidList)) {
                return true;
            }
            if(isOperatorUsedOutsideOrderBy((Node *)when->result, oidList)) {
                return true;
            }
        }
        if(isOperatorUsedOutsideOrderBy((Node *)caseExpr->defresult, oidList)) {
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
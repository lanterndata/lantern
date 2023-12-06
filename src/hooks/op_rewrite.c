#include <postgres.h>

#include "op_rewrite.h"

#include <access/genam.h>
#include <assert.h>
#include <catalog/pg_amproc.h>
#include <catalog/pg_opclass.h>
#include <commands/defrem.h>
#include <miscadmin.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <stdbool.h>
#include <stdint.h>
#include <utils/catcache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "../hnsw/options.h"
#include "plan_tree_walker.h"
#include "utils.h"

#if PG_VERSION_NUM < 120000
#include <access/heapam.h>
#include <access/htup_details.h>
#else
#include <access/relation.h>
#endif

static Node *operator_rewriting_mutator(Node *node, void *ctx);

void base_plan_mutator(Plan *plan, void *context)
{
    plan->lefttree = (Plan *)operator_rewriting_mutator((Node *)plan->lefttree, context);
    plan->righttree = (Plan *)operator_rewriting_mutator((Node *)plan->righttree, context);
    plan->initPlan = (List *)operator_rewriting_mutator((Node *)plan->initPlan, context);
    // checking qual and target list at the end covers some edge cases, if you modify this leave them here
    plan->qual = (List *)operator_rewriting_mutator((Node *)plan->qual, context);
    plan->targetlist = (List *)operator_rewriting_mutator((Node *)plan->targetlist, context);
}

// recursively descend the plan tree searching for expressions with the <-> operator that are part of a non-index scan
// src/include/nodes/plannodes.h and src/include/nodes/nodes.h contain relevant definitions
Node *plan_tree_mutator(Plan *plan, void *context)
{
    check_stack_depth();

    switch(nodeTag(plan)) {
        case T_SubqueryScan:
        {
            SubqueryScan *subqueryscan = (SubqueryScan *)plan;
            base_plan_mutator(&(subqueryscan->scan.plan), context);
            subqueryscan->subplan = (Plan *)operator_rewriting_mutator((Node *)subqueryscan->subplan, context);
            return (Node *)subqueryscan;
        }
        case T_CteScan:
        {
            CteScan *ctescan = (CteScan *)plan;
            base_plan_mutator(&(ctescan->scan.plan), context);
            return (Node *)ctescan;
        }
#if PG_VERSION_NUM < 160000
        case T_Join:
        {
            Join *join = (Join *)plan;
            base_plan_mutator(&(join->plan), context);
            join->joinqual = (List *)operator_rewriting_mutator((Node *)join->joinqual, context);
            return (Node *)join;
        }
#endif
        case T_NestLoop:
        {
            NestLoop *nestloop = (NestLoop *)plan;
            base_plan_mutator((Plan *)&(nestloop->join), context);
            return (Node *)nestloop;
        }
        case T_Result:
        {
            Result *result = (Result *)plan;
            base_plan_mutator(&(result->plan), context);
            result->resconstantqual = operator_rewriting_mutator((Node *)result->resconstantqual, context);
            return (Node *)result;
        }
        case T_Limit:
        {
            Limit *limit = (Limit *)plan;
            base_plan_mutator(&(limit->plan), context);
            limit->limitOffset = operator_rewriting_mutator((Node *)limit->limitOffset, context);
            limit->limitCount = operator_rewriting_mutator((Node *)limit->limitCount, context);
            return (Node *)limit;
        }
        case T_Append:
        {
            Append *append = (Append *)plan;
            base_plan_mutator(&(append->plan), context);
            append->appendplans = (List *)operator_rewriting_mutator((Node *)append->appendplans, context);
            return (Node *)append;
        }
        // case T_IncrementalSort: // We will eventually support this
        case T_Agg:
        case T_Group:
        case T_Sort:
        case T_Unique:
        case T_SetOp:
        case T_Hash:
        case T_HashJoin:
        case T_WindowAgg:
        case T_LockRows:
        {
            base_plan_mutator(plan, context);
            return (Node *)plan;
        }
        case T_ModifyTable:  // No order by when modifying a table (update/delete etc)
        case T_BitmapAnd:    // We do not provide a bitmap index
        case T_BitmapOr:
        case T_BitmapHeapScan:
        case T_BitmapIndexScan:
        case T_FunctionScan:  // SELECT * FROM fn(x, y, z)
        case T_ValuesScan:    // VALUES (1), (2)
        case T_Material:      // https://stackoverflow.com/questions/31410030/
#if PG_VERSION_NUM >= 140000
        case T_Memoize:  // memoized inner loop must have an index to be memoized
#endif
        case T_WorkTableScan:  // temporary table, shouldn't have index
        case T_ProjectSet:     // "execute set returning functions" feels safe to exclude
        case T_TableFuncScan:  // scan of a function that returns a table, shouldn't have an index
        case T_ForeignScan:    // if the relation is foreign we can't determine if it has an index
        default:
            break;
    }
    return (Node *)plan;
}

// To write syscache calls look for the 'static const struct cachedesc cacheinfo[]' in utils/cache/syscache.c
// These describe the different caches that will be initialized into SysCache and the keys they support in searches
// The anums tell you the table and the column that the key will be compared to this is afaict the only way to match
// them to SQL for example pg_am.oid -> Anum_pg_am_oid the keys must be in order but they need not all be included the
// comment next to the top label is the name of the #defined cacheid that you should use as your first argument you can
// destructure the tuple int a From_(table_name) with GETSTRUCT to pull individual rows out
static Oid get_func_id_from_index(Relation index)
{
    Oid hnswamoid = get_index_am_oid("hnsw", false);
    if(index->rd_rel->relam != hnswamoid) return InvalidOid;

    // indclass is inaccessible on the form data
    // https://www.postgresql.org/docs/current/system-catalog-declarations.html
    bool  isNull;
    Oid   idxopclassoid;
    Datum classDatum = SysCacheGetAttr(INDEXRELID, index->rd_indextuple, Anum_pg_index_indclass, &isNull);
    if(!isNull) {
        oidvector *indclass = (oidvector *)DatumGetPointer(classDatum);
        assert(indclass->dim1 == 1);
        idxopclassoid = indclass->values[ 0 ];
    } else {
        index_close(index, AccessShareLock);
        elog(ERROR, "Failed to retrieve indclass oid from index class");
    }

    // SELECT * FROM pg_opclass WHERE opcmethod=hnswamoid AND opcname=dist_cos_ops
    HeapTuple opclassTuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(idxopclassoid));
    if(!HeapTupleIsValid(opclassTuple)) {
        index_close(index, AccessShareLock);
        elog(ERROR, "Failed to find operator class for key column");
    }

    Oid opclassOid = ((Form_pg_opclass)GETSTRUCT(opclassTuple))->opcfamily;
    ReleaseSysCache(opclassTuple);

    // SELECT * FROM pg_amproc WHERE amprocfamily=opclassOid
    // SearchSysCache1 is what we want and in fact it runs fine against release builds. However debug builds assert that
    // AMPROCNUM takes only 1 arg which isn't true and so they fail. We therefore have to use SearchSysCacheList1 since
    // it doesn't enforce this invariant. Ideally we would call SearchCatCache1 directly but postgres doesn't expose
    // necessary constants
    CatCList *opList = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opclassOid));
    assert(opList->n_members == 1);
    HeapTuple opTuple = &opList->members[ 0 ]->tuple;
    if(!HeapTupleIsValid(opTuple)) {
        index_close(index, AccessShareLock);
        elog(ERROR, "Failed to find the function for operator class");
    }
    Oid functionId = ((Form_pg_amproc)GETSTRUCT(opTuple))->amproc;
    ReleaseCatCacheList(opList);

    return functionId;
}

static Node *operator_rewriting_mutator(Node *node, void *ctx)
{
    OpRewriterContext *context = (OpRewriterContext *)ctx;

    if(node == NULL) return node;

    if(IsA(node, OpExpr)) {
        OpExpr *opExpr = (OpExpr *)node;
        if(list_member_oid(context->ldb_ops, opExpr->opno)) {
            if(context->indices == NULL) {
                return node;
            } else {
                ListCell *lc;
                foreach(lc, context->indices) {
                    uintptr_t intermediate = (uintptr_t)lfirst(lc);
                    Oid       indexid = (Oid)intermediate;
                    Relation  index = index_open(indexid, AccessShareLock);
                    Oid       indexfunc = get_func_id_from_index(index);
                    if(OidIsValid(indexfunc)) {
                        MemoryContext old = MemoryContextSwitchTo(PortalContext);
                        FuncExpr     *fnExpr = makeNode(FuncExpr);
                        fnExpr->funcresulttype = opExpr->opresulttype;
                        fnExpr->funcretset = opExpr->opretset;
                        fnExpr->funccollid = opExpr->opcollid;
                        fnExpr->inputcollid = opExpr->inputcollid;
                        fnExpr->args = opExpr->args;
                        fnExpr->location = opExpr->location;
                        // operators can't take variadic arguments
                        fnExpr->funcvariadic = false;
                        // print it as a function
                        fnExpr->funcformat = COERCE_EXPLICIT_CALL;
                        fnExpr->funcid = indexfunc;
                        MemoryContextSwitchTo(old);

                        index_close(index, AccessShareLock);

                        return (Node *)fnExpr;
                    }
                    index_close(index, AccessShareLock);
                }
                return node;
            }
        }
    }

    if(IsA(node, IndexScan) || IsA(node, IndexOnlyScan)) {
        return node;
    }
    if(IsA(node, SeqScan) || IsA(node, SampleScan)) {
        Scan          *scan = (Scan *)node;
        Plan          *scanPlan = &scan->plan;
        Oid            rtrelid = scan->scanrelid;
        RangeTblEntry *rte = rt_fetch(rtrelid, context->rtable);
        Oid            relid = rte->relid;
        Relation       rel = relation_open(relid, AccessShareLock);
        if(rel->rd_indexvalid) {
            context->indices = RelationGetIndexList(rel);
        }
        relation_close(rel, AccessShareLock);

        base_plan_mutator(scanPlan, context);
        return (Node *)scan;
    }

    if(IsA(node, List)) {
        MemoryContext old = MemoryContextSwitchTo(PortalContext);
        List         *list = (List *)node;
        List         *ret = NIL;
        ListCell     *lc;
        foreach(lc, list) {
            ret = lappend(ret, operator_rewriting_mutator((Node *)lfirst(lc), ctx));
        }
        MemoryContextSwitchTo(old);
        return (Node *)ret;
    }

    if(is_plan_node(node)) {
        return (Node *)plan_tree_mutator((Plan *)node, ctx);
    } else {
        return expression_tree_mutator(node, operator_rewriting_mutator, ctx);
    }
}

bool ldb_rewrite_ops(Plan *plan, List *oidList, List *rtable)
{
    Node *node = (Node *)plan;

    OpRewriterContext context;
    context.ldb_ops = oidList;
    context.indices = NULL;
    context.rtable = rtable;

    if(ldb_pgvector_compat || IsA(node, IndexScan) || IsA(node, IndexOnlyScan)) {
        return false;
    }

    operator_rewriting_mutator(node, (void *)&context);
    return true;
}

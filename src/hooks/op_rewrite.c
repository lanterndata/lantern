#include <postgres.h>

#include "op_rewrite.h"

#include <access/genam.h>
#include <access/relation.h>
#include <assert.h>
#include <catalog/pg_amproc.h>
#include <catalog/pg_opclass.h>
#include <commands/defrem.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <stdbool.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "plan_tree_walker.h"
#include "utils.h"

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
    HeapTuple opTuple = SearchSysCache1(AMPROCNUM, ObjectIdGetDatum(opclassOid));
    if(!HeapTupleIsValid(opTuple)) {
        index_close(index, AccessShareLock);
        elog(ERROR, "Failed to find the function for operator class");
    }
    Oid functionId = ((Form_pg_amproc)GETSTRUCT(opTuple))->amproc;
    ReleaseSysCache(opTuple);

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
                    Oid      indexid = (Oid)lfirst(lc);
                    Relation index = index_open(indexid, AccessShareLock);
                    Oid      indexfunc = get_func_id_from_index(index);
                    if(OidIsValid(indexfunc)) {
                        FuncExpr *fnExpr = makeNode(FuncExpr);
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
    if(IsA(node, SeqScan)) {
        SeqScan *seqscan = (SeqScan *)node;
#if PG_VERSION_NUM >= 150000
        Plan *seqscanplan = &seqscan->scan.plan;
        Oid   rtrelid = seqscan->scan.scanrelid;
#else
        Plan *seqscanplan = &seqscan->plan;
        Oid   rtrelid = secscan->scanrelid;
#endif
        RangeTblEntry *rte = rt_fetch(rtrelid, context->rtable);
        Oid            relid = rte->relid;
        Relation       rel = relation_open(relid, AccessShareLock);
        if(rel->rd_indexvalid) {
            context->indices = RelationGetIndexList(rel);
        }
        relation_close(rel, AccessShareLock);

        base_plan_mutator(seqscanplan, operator_rewriting_mutator, context);
        return (Node *)seqscan;
    }

    if(is_plan_node(node)) {
        return (Node *)plan_tree_mutator((Plan *)node, operator_rewriting_mutator, ctx);
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

    if(IsA(node, IndexScan) || IsA(node, IndexOnlyScan)) {
        return false;
    }

    operator_rewriting_mutator(node, (void *)&context);
    return true;
}

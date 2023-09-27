#include <postgres.h>

#include "plan_tree_walker.h"

#include <miscadmin.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>

bool base_plan_walker(Plan *plan, bool (*walker_func)(Node *plan, void *context), void *context)
{
    if(walker_func((Node *)plan->targetlist, context)) return true;
    if(walker_func((Node *)plan->qual, context)) return true;
    if(walker_func((Node *)plan->lefttree, context)) return true;
    if(walker_func((Node *)plan->righttree, context)) return true;
    if(walker_func((Node *)plan->initPlan, context)) return true;
    return false;
}

bool plan_tree_walker(Plan *plan, bool (*walker_func)(Node *plan, void *context), void *context)
{
    check_stack_depth();

    switch(nodeTag(plan)) {
        case T_SeqScan:
        {
            SeqScan *seqscan = (SeqScan *)plan;
#if PG_VERSION_NUM >= 150000
            Plan seqscanplan = seqscan->scan.plan;
#else
            Plan seqscanplan = seqscan->plan;
#endif
            if(base_plan_walker(&seqscanplan, walker_func, context)) return true;
            break;
        }
        case T_IndexScan:
        {
            IndexScan *indexscan = (IndexScan *)plan;
            if(base_plan_walker(&(indexscan->scan.plan), walker_func, context)) return true;
            if(walker_func((Node *)indexscan->indexqual, context)) return true;
            if(walker_func((Node *)indexscan->indexorderby, context)) return true;
            break;
        }
        case T_IndexOnlyScan:
        {
            IndexOnlyScan *indexonlyscan = (IndexOnlyScan *)plan;
            if(base_plan_walker(&(indexonlyscan->scan.plan), walker_func, context)) return true;
            if(walker_func((Node *)indexonlyscan->indexqual, context)) return true;
            if(walker_func((Node *)indexonlyscan->indexorderby, context)) return true;
            break;
        }
        case T_SubqueryScan:
        {
            SubqueryScan *subqueryscan = (SubqueryScan *)plan;
            if(base_plan_walker(&(subqueryscan->scan.plan), walker_func, context)) return true;
            if(walker_func((Node *)subqueryscan->subplan, context)) return true;
            break;
        }
        case T_CteScan:
        {
            CteScan *ctescan = (CteScan *)plan;
            if(base_plan_walker(&(ctescan->scan.plan), walker_func, context)) return true;
            break;
        }
#if PG_VERSION_NUM < 160000
        case T_Join:
        {
            Join *join = (Join *)plan;
            if(base_plan_walker(&(join->plan), walker_func, context)) return true;
            if(walker_func((Node *)join->joinqual, context)) return true;
            break;
        }
#endif
        case T_Agg:
        {
            Agg *agg = (Agg *)plan;
            if(base_plan_walker(&(agg->plan), walker_func, context)) return true;
            break;
        }
        case T_Group:
        {
            Group *group = (Group *)plan;
            if(base_plan_walker(&(group->plan), walker_func, context)) return true;
            break;
        }
        case T_Sort:
        {
            Sort *sort = (Sort *)plan;
            if(base_plan_walker(&(sort->plan), walker_func, context)) return true;
            break;
        }
        case T_Unique:
        {
            Unique *unique = (Unique *)plan;
            if(base_plan_walker(&(unique->plan), walker_func, context)) return true;
            break;
        }
        case T_NestLoop:
        {
            NestLoop *nestloop = (NestLoop *)plan;
            if(base_plan_walker((Plan *)&(nestloop->join), walker_func, context)) return true;
            break;
        }
        case T_Result:
        {
            Result *result = (Result *)plan;
            if(base_plan_walker(&(result->plan), walker_func, context)) return true;
            if(walker_func((Node *)result->resconstantqual, context)) return true;
            break;
        }
        case T_Limit:
        {
            Limit *limit = (Limit *)plan;
            if(base_plan_walker(&(limit->plan), walker_func, context)) return true;
            if(walker_func((Node *)limit->limitOffset, context)) return true;
            if(walker_func((Node *)limit->limitCount, context)) return true;
            break;
        }
        case T_Append:
        {
            Append *append = (Append *)plan;
            if(base_plan_walker(&(append->plan), walker_func, context)) return true;
            if(walker_func((Node *)append->appendplans, context)) return true;
            break;
        }
        default:
            return false;
    }
    return false;
}

void base_plan_mutator(Plan *plan, Node *(*mutator_func)(Node *plan, void *context), void *context)
{
    plan->targetlist = (List *)mutator_func((Node *)plan->targetlist, context);
    plan->qual = (List *)mutator_func((Node *)plan->qual, context);
    plan->lefttree = (Plan *)mutator_func((Node *)plan->lefttree, context);
    plan->righttree = (Plan *)mutator_func((Node *)plan->righttree, context);
    plan->initPlan = (List *)mutator_func((Node *)plan->initPlan, context);
}

Node *plan_tree_mutator(Plan *plan, Node *(*mutator_func)(Node *plan, void *context), void *context)
{
    check_stack_depth();

    switch(nodeTag(plan)) {
        case T_SeqScan:
        {
            SeqScan *seqscan = (SeqScan *)plan;
#if PG_VERSION_NUM >= 150000
            Plan *seqscanplan = &seqscan->scan.plan;
#else
            Plan *seqscanplan = &seqscan->plan;
#endif
            base_plan_mutator(seqscanplan, mutator_func, context);
            return (Node *)seqscan;
        }
        case T_IndexScan:
        {
            IndexScan *indexscan = (IndexScan *)plan;
            base_plan_mutator(&(indexscan->scan.plan), mutator_func, context);
            indexscan->indexqual = (List *)mutator_func((Node *)indexscan->indexqual, context);
            indexscan->indexorderby = (List *)mutator_func((Node *)indexscan->indexorderby, context);
            return (Node *)indexscan;
        }
        case T_IndexOnlyScan:
        {
            IndexOnlyScan *indexonlyscan = (IndexOnlyScan *)plan;
            base_plan_mutator(&(indexonlyscan->scan.plan), mutator_func, context);
            indexonlyscan->indexqual = (List *)mutator_func((Node *)indexonlyscan->indexqual, context);
            indexonlyscan->indexorderby = (List *)mutator_func((Node *)indexonlyscan->indexorderby, context);
            return (Node *)indexonlyscan;
        }
        case T_SubqueryScan:
        {
            SubqueryScan *subqueryscan = (SubqueryScan *)plan;
            base_plan_mutator(&(subqueryscan->scan.plan), mutator_func, context);
            subqueryscan->subplan = (Plan *)mutator_func((Node *)subqueryscan->subplan, context);
            return (Node *)subqueryscan;
        }
        case T_CteScan:
        {
            CteScan *ctescan = (CteScan *)plan;
            base_plan_mutator(&(ctescan->scan.plan), mutator_func, context);
            return (Node *)ctescan;
        }
#if PG_VERSION_NUM < 160000
        case T_Join:
        {
            Join *join = (Join *)plan;
            base_plan_mutator(&(join->plan), mutator_func, context);
            join->joinqual = (List *)mutator_func((Node *)join->joinqual, context);
            return (Node *)join;
        }
#endif
        case T_Agg:
        {
            Agg *agg = (Agg *)plan;
            base_plan_mutator(&(agg->plan), mutator_func, context);
            return (Node *)agg;
        }
        case T_Group:
        {
            Group *group = (Group *)plan;
            base_plan_mutator(&(group->plan), mutator_func, context);
            return (Node *)group;
        }
        case T_Sort:
        {
            Sort *sort = (Sort *)plan;
            base_plan_mutator(&(sort->plan), mutator_func, context);
            return (Node *)sort;
        }
        case T_Unique:
        {
            Unique *unique = (Unique *)plan;
            base_plan_mutator(&(unique->plan), mutator_func, context);
            return (Node *)unique;
        }
        case T_NestLoop:
        {
            NestLoop *nestloop = (NestLoop *)plan;
            base_plan_mutator((Plan *)&(nestloop->join), mutator_func, context);
            return (Node *)nestloop;
        }
        case T_Result:
        {
            Result *result = (Result *)plan;
            base_plan_mutator(&(result->plan), mutator_func, context);
            result->resconstantqual = mutator_func((Node *)result->resconstantqual, context);
            return (Node *)result;
        }
        case T_Limit:
        {
            Limit *limit = (Limit *)plan;
            base_plan_mutator(&(limit->plan), mutator_func, context);
            limit->limitOffset = mutator_func((Node *)limit->limitOffset, context);
            limit->limitCount = mutator_func((Node *)limit->limitCount, context);
            return (Node *)limit;
        }
        case T_Append:
        {
            Append *append = (Append *)plan;
            base_plan_mutator(&(append->plan), mutator_func, context);
            append->appendplans = (List *)mutator_func((Node *)append->appendplans, context);
            return (Node *)append;
        }
        default:
            return (Node *)plan;
    }
    return (Node *)plan;
}

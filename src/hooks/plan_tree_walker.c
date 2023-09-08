#include <postgres.h>

#include <miscadmin.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>

bool plan_tree_walker_util(Plan *plan, bool (*walker_func)(Plan *plan, void *context), void *context)
{
    if(walker_func(plan->targetlist, context)) return true;
    if(walker_func(plan->qual, context)) return true;
    if(walker_func(plan->lefttree, context)) return true;
    if(walker_func(plan->righttree, context)) return true;
    if(walker_func(plan->initPlan, context)) return true;
    return false;
}

bool plan_tree_walker(Plan *plan, bool (*walker_func)(Plan *plan, void *context), void *context)
{
    if(plan == NULL) return false;

    check_stack_depth();

    switch(nodeTag(plan)) {
        case T_List:
            List     *list = (List *)plan;
            ListCell *lc;
            foreach(lc, list) {
                if(plan_tree_walker(lfirst(lc), walker_func, context)) return true;
            }
            break;

        // Scan nodes
        case T_SeqScan:
            SeqScan *seqscan = (SeqScan *)plan;
            if(plan_tree_walker_util(&(seqscan->scan.plan), walker_func, context)) return true;
            break;
        case T_IndexScan:
            IndexScan *indexscan = (IndexScan *)plan;
            if(plan_tree_walker_util(&(indexscan->scan.plan), walker_func, context)) return true;
            if(walker_func(indexscan->indexqual, context)) return true;
            if(walker_func(indexscan->indexorderby, context)) return true;
            break;
        case T_IndexOnlyScan:
            IndexOnlyScan *indexonlyscan = (IndexOnlyScan *)plan;
            if(plan_tree_walker_util(&(indexonlyscan->scan.plan), walker_func, context)) return true;
            if(walker_func(indexonlyscan->indexqual, context)) return true;
            if(walker_func(indexonlyscan->indexorderby, context)) return true;
            break;

        // Join nodes
        case T_Join:
            Join *join = (Join *)plan;
            if(plan_tree_walker_util(&(join->plan), walker_func, context)) return true;
            if(walker_func(join->joinqual, context)) return true;
            break;

        // Nodes dealing with aggregation / grouping / sorting
        case T_Group:
            Group *group = (Group *)plan;
            if(plan_tree_walker_util(&(group->plan), walker_func, context)) return true;
            break;
        case T_Sort:
            Sort *sort = (Sort *)plan;
            if(plan_tree_walker_util(&(sort->plan), walker_func, context)) return true;
            break;

        // Singleton Nodes
        case T_Result:
            Result *result = (Result *)plan;
            if(plan_tree_walker_util(&(result->plan), walker_func, context)) return true;
            if(walker_func(result->resconstantqual, context)) return true;
            break;
        case T_Limit:
            Limit *limit = (Limit *)plan;
            if(plan_tree_walker_util(&(limit->plan), walker_func, context)) return true;
            if(walker_func(limit->limitOffset, context)) return true;
            if(walker_func(limit->limitCount, context)) return true;
            break;

        default:
            return false;
    }
    return false;
}

#include <postgres.h>

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <parser/parse_oper.h>
#include <utils/builtins.h>
#include <catalog/namespace.h>



List *ldb_get_operator_oids()
{
    List *oidList = NIL;

    List *nameList = lappend(NIL, makeString("<->"));

    Oid intOperator = LookupOperName(NULL, nameList, INT4ARRAYOID, INT4ARRAYOID, true, -1);
    Oid floatOperator = LookupOperName(NULL, nameList, FLOAT4ARRAYOID, FLOAT4ARRAYOID, true, -1);

    if(OidIsValid(intOperator)) {
        oidList = lappend_oid(oidList, intOperator);
    }
    if(OidIsValid(floatOperator)) {
        oidList = lappend_oid(oidList, floatOperator);
    }

    list_free(nameList);

    return oidList;
}

List *ldb_get_distance_function_oids()
{
    List *oidList = NIL;

    Oid l2sq_dist_oid = InvalidOid;
    Oid cos_dist_oid = InvalidOid;
    Oid hamming_dist_oid = InvalidOid;
    FuncCandidateList funcList1 = FuncnameGetCandidates(list_make1(makeString("l2sq_dist")), -1, NIL, false, false, false, true);
    if (funcList1 != NULL) {
        l2sq_dist_oid = funcList1->oid;
    } else {
        elog(WARNING, "l2sq_dist was not found.");
    }
    FuncCandidateList funcList2 = FuncnameGetCandidates(list_make1(makeString("cos_dist")), -1, NIL, false, false, false, true);
    if (funcList2 != NULL) {
        cos_dist_oid = funcList2->oid;
    } else {
        elog(WARNING, "cos_dist was not found.");
    }
    FuncCandidateList funcList3 = FuncnameGetCandidates(list_make1(makeString("hamming_dist")), -1, NIL, false, false, false, true);
    if (funcList2 != NULL) {
        hamming_dist_oid = funcList3->oid;
    } else {
        elog(WARNING, "hamming_dist was not found.");
    }

    if(OidIsValid(l2sq_dist_oid)) {
        oidList = lappend_oid(oidList, l2sq_dist_oid);
    }
    if(OidIsValid(cos_dist_oid)) {
        oidList = lappend_oid(oidList, cos_dist_oid);
    }
    if(OidIsValid(hamming_dist_oid)) {
        oidList = lappend_oid(oidList, hamming_dist_oid);
    }

    return oidList;
}
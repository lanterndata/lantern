#include <postgres.h>

#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <parser/parse_oper.h>

List *ldb_get_operator_oids()
{
    List *oidList = NIL;

    List *nameList = lappend(NIL, makeString("<?>"));

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

#include <postgres.h>

#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>

List *get_operator_oids()
{
    List *oidList = NIL;

    Oid intArrayOid = INT4ARRAYOID;
    Oid floatArrayOid = FLOAT4ARRAYOID;

    List *nameList = lappend(NIL, makeString("<->"));

    Oid intOperator = LookupOperName(NULL, nameList, intArrayOid, intArrayOid, true, -1);
    Oid floatOperator = LookupOperName(NULL, nameList, floatArrayOid, floatArrayOid, true, -1);

    if(OidIsValid(intOperator)) {
        oidList = lappend_oid(oidList, intOperator);
    }
    if(OidIsValid(floatOperator)) {
        oidList = lappend_oid(oidList, floatOperator);
    }

    list_free(nameList);

    return oidList;
}
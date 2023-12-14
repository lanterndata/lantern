#include <libpq-fe.h>

#include "runner.h"

int test_op_rewrite(TestCaseState *state)
{
    PGconn *conn = state->conn;

    const char *query
        = "SELECT tablename, reltuples "
          "FROM pg_tables "
          "JOIN pg_class ON pg_tables.tablename = pg_class.relname "
          "WHERE schemaname = $1;";

    PGresult *res = PQexec(conn, "DROP TABLE IF EXISTS _lantern_test_op");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to drop table: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    res = PQexec(conn, "CREATE TABLE _lantern_test_op (id INT, name TEXT, v REAL[])");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create table: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }
    PQclear(res);

    res = PQexec(conn, "INSERT INTO _lantern_test_op(id, name, v) VALUES (1, 'n1', '{1,1}')");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        PQclear(res);
        return 1;
    }
    PQclear(res);

    res = PQexec(conn, "CREATE INDEX ON _lantern_test_op USING hnsw(v)");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create index: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    const char *name = "public";

    res = PQexecParams(conn, query, 1, NULL, &name, NULL, NULL, 0);
    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to execute query: %s\n", PQerrorMessage(conn));
        return 1;
    }
    return 0;
}

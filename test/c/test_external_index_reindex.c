#include <libpq-fe.h>
#include <stdlib.h>

int test_external_index_reindex(PGconn *conn)
{
    system("cp /tmp/lantern/files/index-sift1k-l2.usearch /tmp/lantern/files/index-reindex.usearch");

    PGresult *res = PQexec(conn, "DROP TABLE IF EXISTS sift_base1k");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to drop table: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    res = PQexec(conn, "CREATE TABLE IF NOT EXISTS sift_base1k (id SERIAL, v REAL[])");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create table: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }
    PQclear(res);

    res = PQexec(conn, "COPY sift_base1k (v) FROM '/tmp/lantern/vector_datasets/sift_base1k_arrays.csv' WITH csv;");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to copy to table: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }
    PQclear(res);

    // Verify that REINDEX is not working if index params are no passed
    // And file does not exist
    res = PQexec(conn,
                 "CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH "
                 "(_experimental_index_path='/tmp/lantern/files/index-reindex.usearch');");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create index: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    system("rm -f /tmp/lantern/files/index-reindex.usearch");
    res = PQexec(conn, "REINDEX INDEX hnsw_l2_index");

    if(PQresultStatus(res) == PGRES_COMMAND_OK) {
        fprintf(stderr, "Reindex should have failed but it was successfull");
        PQclear(res);
        return 1;
    }

    res = PQexec(conn, "DROP INDEX hnsw_l2_index");

    system("cp /tmp/lantern/files/index-sift1k-l2.usearch /tmp/lantern/files/index-reindex.usearch");
    // Verify that REINDEX is working if index params are passed (Fallback to local index creation)
    res = PQexec(conn,
                 "CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH "
                 "(_experimental_index_path='/tmp/lantern/files/index-reindex.usearch', m=16, ef=32, "
                 "ef_construction=64, dim=128);");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create index: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    system("rm -f /tmp/lantern/files/index-reindex.usearch");
    res = PQexec(conn, "REINDEX INDEX hnsw_l2_index");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to reindex index: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    res = PQexec(conn, "SELECT _lantern_internal.validate_index('hnsw_l2_index', false);");
    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }

    return 0;
}

#include <libpq-fe.h>
#include <stdlib.h>
#include <unistd.h>

#include "runner.h"

int replica_test_index(TestCaseState* state)
{
    // Create table and index
    PGresult* res
        = PQexec(state->conn,
                 "CREATE FUNCTION prepare(create_index BOOL) RETURNS VOID AS $$\n"
                 "BEGIN\n"
                 "    DROP TABLE IF EXISTS small_world;\n"
                 "    CREATE TABLE small_world (id SERIAL PRIMARY KEY, v real[]);\n"
                 "    IF create_index THEN\n"
                 "        CREATE INDEX ON small_world USING lantern_hnsw (v) WITH (dim=3);\n"
                 "    END IF;\n"
                 "    -- let's insert HNSW_BLOCKMAP_BLOCKS_PER_PAGE (2000) record to fill the first blockmap page\n"
                 "    BEGIN\n"
                 "        FOR i IN 1..2000 LOOP\n"
                 "            INSERT INTO small_world (v) VALUES (array_replace(ARRAY[0,0,-1], -1, i));\n"
                 "        END LOOP;\n"
                 "    END;\n"
                 "END;\n"
                 "$$ LANGUAGE plpgsql VOLATILE;");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create prepare function: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    res = PQexec(state->conn,
                 "SELECT prepare(FALSE);"
                 "CREATE INDEX ON small_world USING lantern_hnsw (v) WITH (dim=3);"
                 "CHECKPOINT;");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to insert data: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    res = PQexec(state->conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on master: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    sleep(2);  // wait for replica to sync
    res = PQexec(state->replica_conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on replica: %s\n", PQerrorMessage(state->replica_conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    res = PQexec(state->replica_conn, "SELECT v <-> '{1,1,1}' FROM small_world ORDER BY v <-> '{1,1,1}' LIMIT 10;");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to query index on replica: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Insert more data and crash replica
    res = PQexec(state->conn,
                 "SELECT prepare(TRUE);"
                 "INSERT INTO small_world (v) VALUES ('{2,2,2}'), ('{2,2,2}'), ('{2,2,2}'), ('{2,2,2}'),"
                 "('{2,2,2}');");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to insert data: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    // Restart replica with crash and verify it start correctly after WAL recovery
    system("bash -c '. ../ci/scripts/bitnami-utils.sh && crash_and_restart_postgres_replica'");
    state->replica_conn = connect_database(
        state->DB_HOST, state->REPLICA_PORT, state->DB_USER, state->DB_PASSWORD, state->TEST_DB_NAME);

    res = PQexec(state->replica_conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on replica after restart: %s\n", PQerrorMessage(state->replica_conn));
        // Tail the log file to see crash error if any
        system("tail /tmp/postgres-slave-conf/pg.log 2>/dev/null || true");
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Restart master with crash and verify it start correctly after WAL recovery
    system("bash -c '. ../ci/scripts/bitnami-utils.sh && crash_and_restart_postgres_master'");
    state->conn
        = connect_database(state->DB_HOST, state->DB_PORT, state->DB_USER, state->DB_PASSWORD, state->TEST_DB_NAME);

    res = PQexec(state->replica_conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on master after restart: %s\n", PQerrorMessage(state->conn));
        // Tail the log file to see crash error if any
        system("tail /tmp/postgres-master-conf/pg.log 2>/dev/null || true");
        PQclear(res);
        return 1;
    }

    PQclear(res);

    return 0;
}

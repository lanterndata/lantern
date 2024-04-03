#include <libpq-fe.h>
#include <stdlib.h>
#include <unistd.h>

#include "runner.h"

int replica_test_unlogged(TestCaseState* state)
{
    /*
    Test Outline
    =============
    1. Create unlogged table and index on it (and insert data)
    2. Make table logged
    3. Insert data on master
    4. Crash and restart slave and call validate_index on it
    */

    PGresult* res;
    int       status;

    // Create unlogged table, index, and insert data
    res = PQexec(state->conn,
                 "DROP TABLE IF EXISTS small_world;"
                 "CREATE UNLOGGED TABLE small_world (id SERIAL PRIMARY KEY, v real[]);"
                 "CREATE INDEX ON small_world USING lantern_hnsw (v) WITH (dim=3);"
                 "INSERT INTO small_world (v) VALUES (ARRAY[0,0,1]), (ARRAY[0,1,0]), (ARRAY[1,0,0]);"
                 "CHECKPOINT;");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr,
                "Failed to prepare unlogged table, create index, and insert data on it: %s\n",
                PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Validate index on master
    res = PQexec(state->conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on master: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Alter table to be logged
    res = PQexec(state->conn, "ALTER TABLE small_world SET LOGGED;");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to alter unlogged table to logged: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Insert some more data
    res = PQexec(state->conn, "INSERT INTO small_world (v) VALUES (ARRAY[1,2,3])");

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to insert more data into the now logged table: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Validate index on master after changing table to be logged and inserting data
    res = PQexec(state->conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on master: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    sleep(2);  // wait for replica to sync

    // Validate index on replica
    res = PQexec(state->replica_conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', false);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on replica: %s\n", PQerrorMessage(state->replica_conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Test query on replica
    res = PQexec(state->replica_conn, "SELECT v <-> '{1,1,1}' FROM small_world ORDER BY v <-> '{1,1,1}' LIMIT 10;");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to query index on replica: %s\n", PQerrorMessage(state->conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);

    // Crash replica:
    status = system("bash -c '. ../ci/scripts/bitnami-utils.sh && crash_and_restart_postgres_replica'");
    expect(0 == status, "Failed to crash and restart replica");
    state->replica_conn = connect_database(
        state->DB_HOST, state->REPLICA_PORT, state->DB_USER, state->DB_PASSWORD, state->TEST_DB_NAME);

    // Validate index on replica after crash
    res = PQexec(state->replica_conn, "SELECT _lantern_internal.validate_index('small_world_v_idx', true);");

    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to validate index on replica after restart: %s\n", PQerrorMessage(state->replica_conn));
        // Tail the log file to see crash error if any
        status = system("tail /tmp/postgres-slave-conf/pg.log 2>/dev/null || true");
        expect(0 == status, "Failed to tail log file");
        PQclear(res);
        return 1;
    }

    PQclear(res);

    return 0;
}

#include "runner.h"

#include <libpq-fe.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Include your test files here
#include "replica_test_index.c"
#include "test_op_rewrite.c"
// ===========================

PGconn *connect_database(
    const char *db_host, const char *db_port, const char *db_user, const char *db_password, const char *db_name)
{
    const int const_db_uri_chars = strlen("host= port= user= dbname= sslmode=disable password=") + 1;
    char *db_uri = malloc(strlen(db_host) + strlen(db_port) + strlen(db_user) + strlen(db_password) + strlen(db_name)
                          + const_db_uri_chars);
    sprintf(db_uri,
            "host=%s port=%s user=%s dbname=%s sslmode=disable password=%s",
            db_host,
            db_port,
            db_user,
            db_name,
            db_password);

    PGconn *conn = PQconnectdb(db_uri);
    free(db_uri);

    if(PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return NULL;
    }
    return conn;
}

int recreate_database(PGconn *root_conn, const char *test_db_name)
{
    char *statement = "DROP DATABASE IF EXISTS ";
    char *full_statement = malloc(strlen(statement) + strlen(test_db_name) + 1);
    sprintf(full_statement, "%s%s", statement, test_db_name);
    PGresult *res = PQexec(root_conn, full_statement);
    free(full_statement);

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to drop test db: %s\n", PQerrorMessage(root_conn));
        PQclear(res);
        return 1;
    }

    statement = "CREATE DATABASE ";
    full_statement = malloc(strlen(statement) + strlen(test_db_name) + 1);
    sprintf(full_statement, "%s%s", statement, test_db_name);
    res = PQexec(root_conn, full_statement);
    free(full_statement);

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create test db: %s\n", PQerrorMessage(root_conn));
        PQclear(res);
        return 1;
    }

    PQclear(res);
    return 0;
}

int create_extension(PGconn *conn)
{
    PGresult *res = PQexec(conn, "CREATE EXTENSION IF NOT EXISTS lantern");
    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to create extension: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return 1;
    }
    PQclear(res);
    return 0;
}

const char *getenv_or_default(const char *env_name, const char *default_val)
{
    const char *val = getenv(env_name);

    if(val == NULL) {
        return default_val;
    }

    return val;
}

int main()
{
    size_t               i;
    struct TestCase      current_case = {0};
    struct TestCaseState current_case_state = {0};
    struct TestCase      test_cases[] = {
        // Add new test files here to be run
        {.name = "test_op_rewrite", .func = (TestCaseFunction)test_op_rewrite},
        {.name = "replica_test_index", .func = (TestCaseFunction)replica_test_index}
        // ================================
    };

    // Set up database connection variables
    const char *DB_HOST = getenv_or_default("DB_HOST", "localhost");
    const char *DB_PORT = getenv_or_default("DB_PORT", "5432");
    const char *DB_USER = getenv_or_default("DB_USER", "postgres");
    const char *REPLICA_PORT = getenv_or_default("REPLICA_PORT", "5433");
    const char *ENABLE_REPLICA = getenv_or_default("ENABLE_REPLICA", NULL);
    const char *DB_PASSWORD = getenv_or_default("DB_PASSWORD", "");
    const char *TEST_DB_NAME = getenv_or_default("TEST_DB_NAME", "lantern_testdb");
    const char *ROOT_DB_NAME = "postgres";
    PGconn     *root_conn = NULL;

    root_conn = connect_database(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, ROOT_DB_NAME);

    if(root_conn == NULL) {
        return 1;
    }

    for(i = 0; i < sizeof(test_cases) / sizeof(struct TestCase); i++) {
        current_case = test_cases[ i ];
        current_case_state.REPLICA_PORT = REPLICA_PORT;
        current_case_state.DB_PORT = DB_PORT;
        current_case_state.DB_HOST = DB_HOST;
        current_case_state.DB_PASSWORD = DB_PASSWORD;
        current_case_state.DB_USER = DB_USER;
        current_case_state.TEST_DB_NAME = TEST_DB_NAME;

        printf("[+] Running test case '%s'...\n", current_case.name);

        // Create test database
        if(recreate_database(root_conn, TEST_DB_NAME)) {
            fprintf(stderr, "[X] Failed to recreate test database\n");
            return 1;
        }

        // Connect replica database
        if(ENABLE_REPLICA != NULL && strcmp(ENABLE_REPLICA, "1") == 0) {
            if(strncmp(current_case.name, "replica_", strlen("replica_")) != 0) {
                // if test case does not start with replica_ prefix skip test
                printf("[+] Skipping test case '%s' in replica mode\n", current_case.name);
                continue;
            }
            // Wait for replica to sync with master or test db will not exist
            sleep(3);
            current_case_state.replica_conn
                = connect_database(DB_HOST, REPLICA_PORT, DB_USER, DB_PASSWORD, TEST_DB_NAME);
            if(current_case_state.replica_conn == NULL) {
                fprintf(stderr, "[X] Can not connect to replica database on port '%s'\n", REPLICA_PORT);
                return 1;
            }
        } else if(strncmp(current_case.name, "replica_", strlen("replica_")) == 0) {
            // if test case does start with replica_ prefix skip test in non replica mode
            printf("[+] Skipping test case '%s' in non replica mode\n", current_case.name);
            continue;
        }

        // Connect to test database
        current_case_state.conn = connect_database(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, TEST_DB_NAME);

        if(current_case_state.conn == NULL) {
            return 1;
        }

        // Create lantern extension
        if(create_extension(current_case_state.conn)) {
            fprintf(stderr, "[X] Failed to create extension\n");
            return 1;
        }

        // Execute test case
        if(current_case.func(&current_case_state)) {
            fprintf(stderr, "[X] Test case '%s' failed\n", current_case.name);
            PQfinish(current_case_state.conn);
            if(ENABLE_REPLICA) {
                PQfinish(current_case_state.replica_conn);
            }
            PQfinish(root_conn);
            return 1;
        }

        // Close test connection
        PQfinish(current_case_state.conn);
        if(ENABLE_REPLICA) {
            PQfinish(current_case_state.replica_conn);
        }
        printf("[+] Test case '%s' passed\n", current_case.name);
    }

    PQfinish(root_conn);
    printf("[+] All tests passed\n");
    return 0;
}

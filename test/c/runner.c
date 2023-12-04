#include <libpq-fe.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Include your test files here
#include "test_op_rewrite.c"
// ===========================

typedef int (*TestCaseFunction)(PGconn *);

struct TestCase
{
    char            *name;
    TestCaseFunction func;
};

PGconn *connect_database(
    const char *db_host, const char *db_port, const char *db_user, const char *db_password, const char *db_name)
{
    const int const_db_uri_chars = strlen("host= port= user= dbname= sslmode=disable password=");
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
    char *full_statement = malloc(strlen(statement) + strlen(test_db_name));
    sprintf(full_statement, "%s%s", statement, test_db_name);
    PGresult *res = PQexec(root_conn, full_statement);
    free(full_statement);

    if(PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Failed to drop test db: %s\n", PQerrorMessage(root_conn));
        PQclear(res);
        return 1;
    }

    statement = "CREATE DATABASE ";
    full_statement = malloc(strlen(statement) + strlen(test_db_name));
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
    size_t          i;
    struct TestCase current_case;
    struct TestCase test_cases[] = {
        // Add new test files here to be run
        {.name = "test_op_rewrite", .func = (TestCaseFunction)test_op_rewrite}
        // ================================
    };

    // Set up database connection variables
    const char *DB_HOST = getenv_or_default("DB_HOST", "localhost");
    const char *DB_PORT = getenv_or_default("DB_PORT", "5432");
    const char *DB_USER = getenv_or_default("DB_USER", "postgres");
    const char *DB_PASSWORD = getenv_or_default("DB_PASSWORD", "");
    const char *TEST_DB_NAME = getenv_or_default("TEST_DB_NAME", "lantern_testdb");
    const char *ROOT_DB_NAME = "postgres";
    PGconn     *test_conn = NULL;
    PGconn     *root_conn = NULL;

    root_conn = connect_database(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, ROOT_DB_NAME);

    if(root_conn == NULL) {
        return 1;
    }

    for(i = 0; i < sizeof(test_cases) / sizeof(struct TestCase); i++) {
        current_case = test_cases[ i ];
        printf("[+] Running test case '%s'...\n", current_case.name);

        // Create test database
        if(recreate_database(root_conn, TEST_DB_NAME)) {
            fprintf(stderr, "[X] Failed to recreate test database\n");
            return 1;
        }

        // Connect to test database
        test_conn = connect_database(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, TEST_DB_NAME);

        if(test_conn == NULL) {
            return 1;
        }

        // Create lantern extensionk
        if(create_extension(test_conn)) {
            fprintf(stderr, "[X] Failed to create extension\n");
            return 1;
        }

        // Execute test case
        if(current_case.func(test_conn)) {
            fprintf(stderr, "[X] Test case '%s' failed\n", current_case.name);
            PQfinish(test_conn);
            PQfinish(root_conn);
            return 1;
        }

        // Close test connection
        PQfinish(test_conn);
        printf("[+] Test case '%s' passed\n", current_case.name);
    }

    PQfinish(root_conn);
    printf("[+] All tests passed\n");
    return 0;
}

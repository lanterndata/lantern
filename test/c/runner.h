#ifndef RUNNER_H
#define RUNNER_H

#include <libpq-fe.h>
typedef struct TestCaseState
{
    PGconn     *conn;
    PGconn     *replica_conn;
    const char *DB_HOST;
    const char *DB_PORT;
    const char *DB_USER;
    const char *REPLICA_PORT;
    const char *DB_PASSWORD;
    const char *TEST_DB_NAME;
} TestCaseState;

typedef int (*TestCaseFunction)(TestCaseState *);

struct TestCase
{
    char            *name;
    TestCaseFunction func;
};

PGconn *connect_database(
    const char *db_host, const char *db_port, const char *db_user, const char *db_password, const char *db_name);

#endif

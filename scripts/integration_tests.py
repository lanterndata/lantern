import pytest
import testgres
import os
import sys
import signal
import warnings
import logging
import socket
import subprocess
import time
from packaging.version import Version

# for pry
import inspect
import code

LOGGER = logging.getLogger(__name__)


def pry():
    frame = inspect.currentframe().f_back
    try:
        code.interact(local=dict(frame.f_globals, **frame.f_locals))
    finally:
        del frame


scripts_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(f"{scripts_path}/../test/sql")
DIST_OPS = {
    "l2sq": "<->",
    "cos": "<=>",
}
# configure testgres logging
logging.basicConfig(filename="/tmp/testgres.log")
# uncomment below to see all postgres logs
testgres.configure_testgres(use_python_logging=True)


# Fixture to create a testgres node, scoped to the session
@pytest.fixture(scope="session")
def primary():
    LOGGER.info("starting primary")
    # Create and start a new node
    node = testgres.get_new_node()
    node.init()
    node.append_conf("enable_seqscan = off")
    node.append_conf("maintenance_work_mem = '1GB'")
    node.append_conf("lantern.pgvector_compat=FALSE")
    node.append_conf("checkpoint_timeout = '100min'")
    node.append_conf("min_wal_size = '1GB'")
    node.append_conf("checkpoint_completion_target = '0.9'")
    # node.append_conf("bgwriter_lru_maxpages = '0.9'")
    node.start()
    LOGGER.info(f"done starting primary {node}")
    # Fresh setup for the database
    node.execute("DROP DATABASE IF EXISTS testdb")
    node.execute("CREATE DATABASE testdb")

    # Run SQL scripts
    node.safe_psql(dbname="testdb", filename="./utils/small_world_array.sql")
    node.safe_psql(dbname="testdb", filename="./utils/sift1k_array.sql")
    # # delete from sift_base1k if id > 100
    # node.execute("testdb", "DELETE FROM sift_Base1k WHERE id > 100")

    node.execute("DROP EXTENSION IF EXISTS lantern", dbname="testdb")
    node.execute("CREATE EXTENSION lantern", dbname="testdb")

    yield node
    # Cleanup: stop the node after all tests are done
    node.stop()


@pytest.fixture(scope="session")
def replica(primary):
    LOGGER.info("attempt at backup")
    with primary.backup(options=["--checkpoint=fast"]) as backup:
        LOGGER.info("created backup")
        replica = backup.spawn_replica("replica").start()
        LOGGER.info("spawned replica")
        yield replica


# Fixture to handle database setup, scoped to function
@pytest.fixture
def tmpdb(primary):
    """
    Fixture to handle a temporary database setup
    """
    # Fresh setup for the database
    primary.execute("DROP DATABASE IF EXISTS testdb")
    primary.execute("CREATE DATABASE testdb")

    # Run SQL scripts
    primary.safe_psql(dbname="testdb", filename="./utils/small_world_array.sql")
    primary.safe_psql(dbname="testdb", filename="./utils/sift1k_array.sql")
    primary.execute("CREATE EXTENSION lantern", dbname="testdb")

    yield primary
    # Optional: cleanup actions after each test if needed


@pytest.fixture(params=["sift_base1k"], scope="session")
def source_table(request):
    return request.param


@pytest.mark.parametrize("distance_metric", ["l2sq", "cos"])
@pytest.mark.parametrize("quant_bits", [32, 16, 8])
@pytest.fixture(scope="session")
def setup_copy_table_with_index(distance_metric, quant_bits, source_table, request):
    table_name = f"{source_table}_{distance_metric}_{quant_bits}"

    primary = request.getfixturevalue("primary")

    # "IF NOT EXISTS" below is necessary because the function is run for replica as well, and I did not find
    # a better way to:
    # 1. Only run these on primary
    # 2. If only replica tests are run, still run these on each correpsonding table in primary, to make sure replica tests work

    # if table_name does not exist, do
    if not primary.execute(
        "testdb",
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')",
    )[0][0]:
        # transorm default SIFT values for the input ranges lantern assumes
        transform = lambda colname: (
            colname
            if quant_bits >= 16
            else f"(SELECT array_agg((el- 50) / 100.0) FROM unnest({colname}) AS t(el))::real[] AS {colname}"
        )
        primary.execute(
            "testdb",
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT id, {transform('v')} FROM {source_table}",
        )
        primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
        primary.execute(
            "testdb",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name} ON {table_name} USING lantern_hnsw (v dist_{distance_metric}_ops) WITH (dim=128, M=8, quant_bits = {quant_bits})",
        )

        LOGGER.info(f"done creating an index on table {table_name}")

    return table_name


def generic_vector_query(
    table_name, distance_metric, kind, query_vector=None, query_vector_id=None
):
    if query_vector_id is not None == query_vector is not None:
        raise ValueError(
            "Either query_vector or query_vector_id should be provided, but not both"
        )

    if query_vector_id is not None:
        assert query_vector is None
        query_vector = f"SELECT v FROM {table_name} WHERE id = {query_vector_id}"

    dist_with_function = f"{distance_metric}_dist(v, ({query_vector}))"
    dist_with_concrete_op = f"v {DIST_OPS[distance_metric]} ({query_vector})"
    dist_with_generic_op = f"v <?> ({query_vector})"

    query_generator = (
        lambda order_by: f"""
        SELECT *,
            {dist_with_function} AS {distance_metric}_dist,
            {dist_with_concrete_op} AS {distance_metric}_op_dist
        FROM {table_name} 
        ORDER BY {order_by}
        LIMIT 10"""
    )
    if kind == "exact":
        return query_generator(dist_with_function)
    elif kind == "concrete":
        return query_generator(dist_with_concrete_op)
    elif kind == "generic":
        return query_generator(dist_with_generic_op)


@pytest.mark.parametrize("distance_metric", ["l2sq", "cos"], scope="session")
@pytest.mark.parametrize("quant_bits", [32, 16, 8, 1], scope="session")
@pytest.mark.parametrize("db", ["primary", "replica"], scope="session")
def test_selects(db, setup_copy_table_with_index, distance_metric, quant_bits, request):
    primary = request.getfixturevalue(db)
    try:
        primary.catchup()
    except:
        pass
    table_name = setup_copy_table_with_index

    q_vec_ids = [1, 3, 5, 10, 20, 55, 72, 11]

    for q_vec_id in q_vec_ids:
        exact_query = generic_vector_query(
            table_name, distance_metric, "exact", query_vector_id=q_vec_id
        )
        concrete_op_query = generic_vector_query(
            table_name, distance_metric, "concrete", query_vector_id=q_vec_id
        )
        generic_op_query = generic_vector_query(
            table_name, distance_metric, "generic", query_vector_id=q_vec_id
        )

        exact_explain_query = f"EXPLAIN {exact_query}"
        exact_plan = primary.execute("testdb", exact_explain_query)
        assert f"Index Scan using idx_{table_name}" not in str(
            exact_plan
        ), f"Exact scan query should not use the vector index. got plan {exact_plan}"

        exact_res = primary.execute("testdb", exact_query)
        exact_ids = [row[0] for row in exact_res]
        assert len(exact_res) > 0, "Expected at least the query vector in the result"
        assert (
            q_vec_id == exact_res[0][0]
        ), "First result in exact query result should be the query vector"

        for query in [generic_op_query, concrete_op_query]:
            explain_query = f"EXPLAIN {query}"
            plan = primary.execute("testdb", explain_query)
            assert f"Index Scan using idx_{table_name}" in str(
                plan
            ), f"Failed for {plan}"
            LOGGER.info(f"running query")

            approx_res = primary.execute("testdb", query)
            approx_ids = [row[0] for row in approx_res]
            LOGGER.info(f"running query")
            highest_dist = float("-inf")

            assert len(approx_res) == len(
                exact_res
            ), f"Exact(={len(exact_res)}and approximate ({len(approx_res)}) queries returned different number of results"
            for i, row in enumerate(approx_res):
                id, vec, dist, op_dist = row
                if i == 0:
                    if quant_bits == 1:
                        assert (
                            id in approx_ids
                        ), f"First result {id} should appear in returned results in bit quantization. result ids: {approx_ids}"
                    else:
                        assert (
                            id == q_vec_id
                        ), f"First result {id} should be the query vector id. result ids: {approx_ids}"
                assert (
                    dist == op_dist
                ), "Distances returned by the operator are not consistent with those of distance function"

                if quant_bits < 32 and dist < highest_dist:
                    warnings.warn(
                        f"Returned distance order flipped: {highest_dist} returned before {dist}. Ensure this is a quantization issue"
                    )
                else:
                    assert dist >= highest_dist
                highest_dist = dist

            # compare recall between exact and approximate results
            recall = len(set(exact_ids).intersection(approx_ids)) / len(exact_ids)
            if quant_bits > 1 and recall < 0.9:
                assert (
                    recall >= 0.7
                ), f"Recall is only {recall} (returned ids: {approx_ids}, exact ids: {exact_ids}"
                warnings.warn(
                    f"Recall is only {recall} (returned ids: {approx_ids}, exact ids: {exact_ids}"
                )
            if quant_bits == 1 and recall < 0.6:
                assert (
                    recall >= 0.4
                ), f"Recall is only {recall} (returned ids: {approx_ids}, exact ids: {exact_ids}"
                warnings.warn(
                    f"Recall is only {recall} (returned ids: {approx_ids}, exact ids: {exact_ids}"
                )


# todo:: something is off with inserts and 1-bit quantization
@pytest.mark.parametrize("distance_metric", ["l2sq", "cos"], scope="session")
@pytest.mark.parametrize("quant_bits", [32, 16, 8], scope="session")
# @pytest.mark.parametrize("db", ["primary", "replica"], scope="session")
def test_inserts(setup_copy_table_with_index, distance_metric, quant_bits, request):
    primary = request.getfixturevalue("primary")
    replica = request.getfixturevalue("replica")
    table_name = setup_copy_table_with_index
    replica.catchup()

    ################################################## INSERTS ON PRIMARY ##################################################
    # catch up the replica to make sure the base relation is synced
    replica.stop()
    primary.execute(
        "testdb",
        f"INSERT INTO {table_name} (id, v) VALUES (4444, (SELECT v FROM {table_name} WHERE id = 44)) ON CONFLICT(id) DO NOTHING",
    )
    primary.execute(
        "testdb",
        f"INSERT INTO {table_name} (id, v) VALUES (4445, (SELECT v FROM {table_name} WHERE id = 44)) ON CONFLICT(id) DO NOTHING",
    )

    assert primary.execute(
        "testdb",
        f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE id = {4444})",
    )[0][0], f"Expected vector with id 4444 to exist in the table"
    # sleep 2 seconds
    primary.execute("SELECT pg_sleep(2)")

    # verify that the rows inserted in the primary do not yet exist on the replica
    primary.stop()
    replica.start()
    assert (
        replica.execute(
            "testdb", f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE id = 4444)"
        )[0][0]
        == False
    ), "Expected vector with id 4444 to not exist in the table"
    assert (
        replica.execute(
            "testdb", f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE id = 4445)"
        )[0][0]
        == False
    ), "Expected vector with id 4445 to not exist in the table"
    primary.start()

    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ INSERTS ON PRIMARY ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    DISTANCE_TOLERANCE_PERCENT = 10 if quant_bits <= 8 else 0

    inserted_ids = [12, 4444, 4445, 44]
    inserted_vector_orig_ids = {
        4444: [4444, 4445, 44],
        44: [4444, 4445, 44],
        4445: [4444, 4445, 44],
        12: [12],
    }

    replica.catchup()

    for q_vec_id in inserted_ids:
        # assert that a vector with id 4444 exists
        assert primary.execute(
            "testdb",
            f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE id = {q_vec_id})",
        )[0][0], f"Expected vector with id 4444 to exist in the table"
        for db in [primary, replica]:
            assert db.execute(
                "testdb",
                f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE id = {q_vec_id})",
            )[0][0], f"Expected vector with id 4444 to exist in the table"

        exact_query = generic_vector_query(
            table_name, distance_metric, "exact", query_vector_id=q_vec_id
        )
        concrete_op_query = generic_vector_query(
            table_name, distance_metric, "concrete", query_vector_id=q_vec_id
        )
        generic_op_query = generic_vector_query(
            table_name, distance_metric, "generic", query_vector_id=q_vec_id
        )

        exact_explain_query = f"EXPLAIN {exact_query}"
        for db in [primary, replica]:
            exact_plan = db.execute("testdb", exact_explain_query)
            assert f"Index Scan using idx_{table_name}" not in str(
                exact_plan
            ), f"Exact scan query should not use the vector index. got plan {exact_plan}"

            exact_res = db.execute("testdb", exact_query)
            assert (
                len(exact_res) > 0
            ), "Expected at least the query vector in the result"
            assert (
                exact_res[0][0] in inserted_vector_orig_ids[q_vec_id]
            ), "First result in exact query result should be the query vector"

            for query in [generic_op_query, concrete_op_query]:
                explain_query = f"EXPLAIN {query}"
                plan = db.execute("testdb", explain_query)
                assert f"Index Scan using idx_{table_name}" in str(
                    plan
                ), f"Failed for {plan}"

                approx_res = db.execute("testdb", query)
                approx_ids = [row[0] for row in approx_res]
                LOGGER.info(f"running query")
                highest_dist = float("-inf")

                assert len(approx_res) == len(
                    exact_res
                ), f"Exact({len(exact_res)}) and approximate ({len(approx_res)}) queries returned different number of results"
                for i, row in enumerate(approx_res):
                    id, vec, dist, op_dist = row
                    if i == 0:
                        if quant_bits == 1:
                            assert (
                                id in approx_ids
                            ), f"First result {id} should appear in returned results in bit quantization. result ids: {approx_ids}"
                        else:
                            assert (
                                id in inserted_vector_orig_ids[id]
                            ), f"First result {id} should be the query vector id {inserted_vector_orig_ids[id]}. result ids: {approx_ids}"
                    assert (
                        dist == op_dist
                    ), "Distances returned by the operator are not consistent with those of distance function"

                    if quant_bits < 32 and dist < highest_dist:
                        warnings.warn(
                            f"Returned distance order flipped: {highest_dist} returned before {dist}. Ensure this is a quantization issue"
                        )

                # assert that all ids equal to the query vector appear in results

                for id in inserted_vector_orig_ids[q_vec_id]:
                    assert (
                        id in approx_ids
                    ), f"Expected id {id} to be in the results: {approx_ids}"


def test_insert_vs_create():
    """
    Create an index on table data and create index on an empty table and insert exact same data.
    Assert that recall and returned vector lists for same queries are roughly compatible
    """


def crash_pg_node(node):
    import psutil

    # kill all children first, checkpointer in particular, to make sure further checkpoints are not done

    # stop all processes, then kill them
    for s in [signal.SIGSTOP, signal.SIGKILL]:
        for child in psutil.Process(node.pid).children():
            # print cmdline
            LOGGER.info(f"Killing child {child.pid} with cmdline {child.cmdline()}")
            os.kill(child.pid, s)

        os.kill(node.pid, s)
    try:
        node.stop()
    except:
        pass


def test_unlogged_table_on_crashes(source_table, request):
    primary = request.getfixturevalue("primary")
    replica = request.getfixturevalue("replica")
    db = primary

    distance_metric = "l2sq"
    quant_bits = 32
    table_name = f"unlogged_{source_table}_{distance_metric}_{quant_bits}"

    primary = request.getfixturevalue("primary")

    # if table_name does not exist, do
    if not primary.execute(
        "testdb",
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')",
    )[0][0]:
        # transorm default SIFT values for the input ranges lantern assumes
        transform = lambda colname: (
            colname
            if quant_bits >= 16
            else f"(SELECT array_agg((el- 50) / 100.0) FROM unnest({colname}) AS t(el))::real[] AS {colname}"
        )
        LOGGER.info(
            f"last checkpoint %s",
            primary.execute(
                "select now()-checkpoint_time as haha, * from pg_control_checkpoint();"
            ),
        )
        LOGGER.info(
            f"checkpoint stats %s", primary.execute("SELECT * FROM pg_stat_bgwriter")
        )
        primary.execute(
            "testdb",
            f"CREATE UNLOGGED TABLE {table_name} AS SELECT id, {transform('v')} FROM {source_table}",
        )
        primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
        primary.execute(
            "testdb",
            f"CREATE INDEX idx_{table_name} ON {table_name} USING lantern_hnsw (v dist_{distance_metric}_ops) WITH (dim=128, M=8, quant_bits = {quant_bits})",
        )
        primary.execute(
            "testdb", f"CREATE INDEX idx_btree_{table_name} ON {table_name}(id)"
        )
        LOGGER.info(
            "unlogged relaiton filepath: %s",
            primary.execute(
                "testdb", f"SELECT pg_relation_filepath('idx_{table_name}')"
            )[0][0],
        )
        LOGGER.info(
            f"LSN after index creation on UNLOGGED %s",
            primary.execute("SELECT pg_current_wal_insert_lsn();"),
        )

        # secondary sync and check unlogged table index WAL size
        replica.catchup()
        # primary contains full index
        assert (
            primary.execute("testdb", f"SELECT pg_relation_size('idx_{table_name}')")[
                0
            ][0]
            > 8192
        ), "Expected index to have >8192 bytes"

        assert (
            replica.execute(
                "testdb", f"SELECT pg_relation_size('idx_btree_{table_name}')"
            )[0][0]
            == 0
        ), "Expected eplica unlogged indexes to be empty"
        assert (
            replica.execute("testdb", f"SELECT pg_relation_size('idx_{table_name}')")[
                0
            ][0]
            == 0
        ), "Expected eplica unlogged indexes to be empty"

        crash_pg_node(primary)
        primary.restart()

        LOGGER.info(
            f"last checkpoint %s",
            primary.execute(
                "select now()-checkpoint_time as haha, * from pg_control_checkpoint();"
            ),
        )
        LOGGER.info(
            f"checkpoint stats %s", primary.execute("SELECT * FROM pg_stat_bgwriter")
        )
        # insert back the data
        primary.execute(
            "testdb",
            f"INSERT INTO {table_name} (id, v) SELECT id, {transform('v')} FROM {source_table}",
        )
        LOGGER.info(f"done creating an index on unlogged table {table_name}")

        crash_pg_node(primary)
        primary.restart()
        # assert unlogged table exists
        assert db.execute(
            "testdb",
            f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')",
        )[0][0], "Expected unlogged table to exist"
        # assert vector index exists on unlogged table
        assert db.execute(
            "testdb",
            f"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '{table_name}' AND indexname = 'idx_{table_name}')",
        )[0][0], "Expected index to exist on unlogged table"
        # assert index relation has 8192 bytes
        # assert db.execute('testdb', f"SELECT pg_relation_size('idx_{table_name}')")[0][0] == 8192, "Expected index to have 8192 bytes"

        # assert unlogged table is empty
        assert (
            primary.execute("testdb", f"SELECT COUNT(*) FROM {table_name}")[0][0] == 0
        ), "Expected unlogged table to be empty after crash"
        # insert into the table
        primary.execute(
            "testdb",
            f"INSERT INTO {table_name} (id, v) VALUES (4444, (SELECT v FROM {source_table} WHERE id = 44)) ON CONFLICT(id) DO NOTHING",
        )


# create table that is a copy of sift_base1k, but  add a column that is a random boolean with 10 percent probability of being true
# then create a hnsw index on the table and run a vector search query with a filter for the boolean value is true


def test_vector_search_with_filter(primary, source_table):
    table_name = f"{source_table}_with_random"
    primary.execute(
        "testdb",
        f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT *, random() < 0.01 AS random_bool FROM {source_table}",
    )
    primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
    # note: when M is too low, the streaming search below might return fewer results since the bottom layer of hnsw is less connected
    primary.execute(
        "testdb",
        f"CREATE INDEX idx_hnsw_{table_name} ON {table_name} USING lantern_hnsw (v dist_l2sq_ops) WITH (dim=128, M=10, quant_bits = 32)",
    )

    limit = 1000
    query_id = 44
    vec_from_id_query = lambda id: f"(SELECT v FROM {table_name} WHERE id = {id})"

    generic_query = (
        lambda filter_val: f"""
    SELECT *, v <-> ({vec_from_id_query(query_id)}) as dist FROM {table_name}
    WHERE random_bool = {filter_val}
    ORDER BY v <-> ({vec_from_id_query(query_id)})
    LIMIT {limit}
    """
    )

    for streaming_search in [0, 1]:
        primary.execute(
            "testdb",
            f"ALTER DATABASE testdb SET lantern_hnsw.streaming_search = {streaming_search}",
        )
        LOGGER.info(f"running streaming search {streaming_search}")
        for filter_val in [True, False]:
            query = generic_query("TRUE" if filter_val else "FALSE")
            plan = primary.execute("testdb", f"EXPLAIN {query}")
            assert f"Index Scan using idx_hnsw_{table_name}" in str(
                plan
            ), f"Failed for {plan}"
            res = primary.execute("testdb", query)
            ids = [row[0] for row in res]

            groundtruth = primary.execute(
                "testdb",
                f"SELECT id FROM {table_name} WHERE random_bool = {filter_val}",
            )
            # db returns each row in a tuple, let's unpack them into a flat list
            groundtruth = [row[0] for row in groundtruth]

            assert len(res) == min(len(groundtruth), limit), (
                "Expected LIMIT to be hit or result set to be less than the limit."
                + f"Some of missing ids: {list(set(groundtruth) - set(ids))[:10]}"
            )
            distances = [row[-1] for row in res]
            # when not doing streaing search, it is possible to return duplicates since
            # we do not track what has been returned
            # Duplicates happen when a wider search returns an element that would be closer to the query
            # than earlier returned eleemnts, so it gets sorted into a position that has already been consumed
            # As a result, an element from a consumed position gets pushed to a non-conusmed position
            if streaming_search == 1:
                assert len(ids) == len(set(ids)), "Expected all results to be unique."

            # If we are filtering for the subset containing the query vector id, assert query vector id is the first result
            if (
                primary.execute(
                    "testdb",
                    f"SELECT random_bool FROM {table_name} WHERE id = {query_id}",
                )[0][0]
                == filter_val
            ):
                assert res[0][0] == query_id, (
                    "Expected first result to be the query vector since it satisfies the filter and is closest to itself"
                    + f" first 10 (id,distance)[:10]={[(id, round(dist, 2)) for id, dist in zip(ids, distances)][:10 ]}"
                )
            else:
                assert (
                    query_id not in ids
                ), "Expected query vector to not be in the results since it does not satisfy the filter"

            # assert (
            #     res[0][0] == 44
            # ), "First result in exact query result should be the query vector"

            # check that all results have random_bool == True
            for row in res:
                assert (
                    row[2] == filter_val
                ), f"Expected all results to have random_bool == {filter_val}"

# fixture to handle external index server setup
@pytest.fixture
def external_index(request):
    cli_path = os.getenv("LANTERN_CLI_PATH")
    use_ssl = os.getenv("USE_SSL") == "1"
    if not cli_path:
        pytest.skip("pass 'LANTERN_CLI_PATH' environment variable to run external indexing tests")
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(('127.0.0.1', 8998)) != 0:
            ssl_args = []
            if use_ssl:
                subprocess.run(["openssl", "req", "-x509", "-nodes", "-days", "365", "-newkey", "rsa:2048", "-keyout", "/tmp/key.pem", "-out", "/tmp/cert.pem", "-subj", "/C=US/ST=California/L=San Francisco/O=Lantern/CN=lantern.dev"])
                ssl_args = ["--cert", "/tmp/cert.pem", "--key", "/tmp/key.pem"]
            subprocess.Popen([cli_path, "start-indexing-server", "--host", "127.0.0.1", "--port", "8998", *ssl_args], shell=False,
             stdin=None, stdout=None, stderr=None, close_fds=True)
            tries = 1
            time.sleep(5)

@pytest.mark.parametrize("distance_metric", ["l2sq", "cos", "hamming"])
@pytest.mark.parametrize("quant_bits", [32, 16, 8, 1])
@pytest.mark.external_index
def test_external_index(external_index, primary, source_table, quant_bits, distance_metric):
    table_name = f"{source_table}_{quant_bits}_{distance_metric}_external_index"
    index_name = f"idx_hnsw_{table_name}_{distance_metric}"
    use_ssl =  "ON" if os.getenv("USE_SSL") == "1" else "OFF"

    data_type = 'INT[]' if distance_metric == 'hamming' else 'REAL[]'

    if not primary.execute(
        "testdb",
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')",
    )[0][0]:
        primary.execute(
            "testdb",
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT id, v::{data_type} FROM {source_table}",
        )
        primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")

    ops = { 'l2sq': 'dist_l2sq_ops', 'cos': 'dist_cos_ops', 'hamming': 'dist_hamming_ops'}[distance_metric]
    # note: when M is too low, the streaming search below might return fewer results since the bottom layer of hnsw is less connected
    primary.execute(
        "testdb",
        f"SET lantern.external_index_secure={use_ssl}; CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true)",
    )

    limit = 1000
    query_id = 44
    vec_from_id_query = lambda id: f"(SELECT v FROM {table_name} WHERE id = {id})"

    op = { 'l2sq': '<->', 'cos': '<=>', 'hamming': '<+>'}[distance_metric]
    query = f"""
    SELECT *, v {op} ({vec_from_id_query(query_id)}) as dist FROM {table_name}
    ORDER BY v {op} ({vec_from_id_query(query_id)})
    LIMIT {limit}
    """

    plan = primary.execute("testdb", f"EXPLAIN {query}")
    assert f"Index Scan using {index_name}" in str(
        plan
    ), f"Failed for {plan}"
    primary.execute("testdb", f"SELECT _lantern_internal.validate_index('{index_name}')")


@pytest.mark.external_index
def test_external_index_pq(external_index, primary, source_table):
    table_name = f"{source_table}_external_index_pq"
    use_ssl =  "ON" if os.getenv("USE_SSL") == "1" else "OFF"
    primary.execute(
        "testdb",
        f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {source_table}",
    )
    primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")

    primary.execute("testdb", f"SELECT quantize_table('{table_name}', 'v', 4, 32, 'l2sq')")

    # note: when M is too low, the streaming search below might return fewer results since the bottom layer of hnsw is less connected
    primary.execute(
        "testdb",
        f"SET lantern.external_index_secure={use_ssl}; CREATE INDEX idx_hnsw_{table_name} ON {table_name} USING lantern_hnsw (v dist_l2sq_ops) WITH (dim=128, M=8, quant_bits = 32, external = true, pq = true)",
    )

    limit = 1000
    query_id = 44
    vec_from_id_query = lambda id: f"(SELECT v FROM {table_name} WHERE id = {id})"

    query = f"""
    SELECT *, v <-> ({vec_from_id_query(query_id)}) as dist FROM {table_name}
    ORDER BY v <-> ({vec_from_id_query(query_id)})
    LIMIT {limit}
    """

    plan = primary.execute("testdb", f"EXPLAIN {query}")
    assert f"Index Scan using idx_hnsw_{table_name}" in str(
        plan
    ), f"Failed for {plan}"
    primary.execute("testdb", f"SELECT _lantern_internal.validate_index('idx_hnsw_{table_name}')")


@pytest.mark.parametrize("distance_metric", ["l2sq"])
@pytest.mark.parametrize("quant_bits", [32])
@pytest.mark.external_index
def test_external_index_reindex(external_index, primary, source_table, quant_bits, distance_metric):
    if primary.version < Version("12.0.0"):
        pytest.skip("REINDEX is not supported on postgres 11")

    table_name = f"{source_table}_{quant_bits}_{distance_metric}_reindx_external_index"
    index_name = f"idx_hnsw_{table_name}_{distance_metric}"
    use_ssl =  "ON" if os.getenv("USE_SSL") == "1" else "OFF"

    data_type = 'INT[]' if distance_metric == 'hamming' else 'REAL[]'

    primary.safe_psql(
        dbname="testdb",
        query=f"ALTER DATABASE testdb SET lantern.external_index_secure={use_ssl}",
    )
    if not primary.execute(
        "testdb",
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')",
    )[0][0]:
        primary.execute(
            "testdb",
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT id, v::{data_type} FROM {source_table}",
        )
        primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")

    ops = { 'l2sq': 'dist_l2sq_ops', 'cos': 'dist_cos_ops', 'hamming': 'dist_hamming_ops'}[distance_metric]
    # note: when M is too low, the streaming search below might return fewer results since the bottom layer of hnsw is less connected
    primary.safe_psql(
        dbname="testdb",
        query=f"CREATE INDEX CONCURRENTLY {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true)",
    )

    limit = 1000
    query_id = 44
    vec_from_id_query = lambda id: f"(SELECT v FROM {table_name} WHERE id = {id})"

    op = { 'l2sq': '<->', 'cos': '<=>', 'hamming': '<+>'}[distance_metric]
    query = f"""
    SELECT *, v {op} ({vec_from_id_query(query_id)}) as dist FROM {table_name}
    ORDER BY v {op} ({vec_from_id_query(query_id)})
    LIMIT {limit}
    """

    plan = primary.execute("testdb", f"EXPLAIN {query}")
    assert f"Index Scan using {index_name}" in str(
        plan
    ), f"Failed for {plan}"
    primary.execute("testdb", f"SELECT _lantern_internal.validate_index('{index_name}')")
    primary.execute("testdb", f"REINDEX INDEX {index_name};")
    primary.execute("testdb", f"SELECT _lantern_internal.validate_index('{index_name}')")

    primary.safe_psql(dbname="testdb", query=f"REINDEX INDEX CONCURRENTLY {index_name};")
    primary.execute("testdb", f"SELECT _lantern_internal.validate_index('{index_name}')")

@pytest.mark.parametrize("distance_metric", ["l2sq"], scope="session")
@pytest.mark.parametrize("quant_bits", [32], scope="session")
@pytest.mark.external_index
def test_external_index_failures(external_index, primary, source_table, quant_bits, distance_metric):
    table_name = f"{source_table}_{quant_bits}_{distance_metric}_external_index_failures"
    index_name = f"idx_hnsw_{table_name}_{distance_metric}_failure"
    use_ssl =  "ON" if os.getenv("USE_SSL") == "1" else "OFF"

    data_type = 'REAL[]'

    if not primary.execute(
        "testdb",
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')",
    )[0][0]:
        primary.execute(
            "testdb",
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT id, v::{data_type} FROM {source_table}",
        )
        primary.execute("testdb", f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")

    ops = 'dist_l2sq_ops'
    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('set_read_timeout', 'crash_after_set_recv_timeout', 0);
                        SET lantern.external_index_secure={use_ssl}; 
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"failed to set receive timeout for socket" in str(e), f"Failed for recv timeout"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('set_write_timeout', 'crash_after_set_send_timeout', 0);
                        SET lantern.external_index_secure={use_ssl}; 
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"failed to set send timeout for socket" in str(e), f"Failed for send timeout"
        
    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_get_flags', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"
        
    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_set_non_blocking', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_connect', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"
        
    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_select', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_on_timeout', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"
        

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_getsockopts', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_getsockopts_err', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('connect_with_timeout', 'crash_after_set_blocking', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: connect timeout" in str(e), f"Failed for connect_with_timeout"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('create_external_index_session', 'crash_on_check_little_endian', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external indexing is supported only for little endian byte ordering" in str(e), f"Failed for is_little_endian()"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('create_external_index_session', 'crash_after_socket_create', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index: socket creation failed" in str(e), f"Failed for create_socket"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('create_external_index_session', 'crash_on_protocol_version_check', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index protocol version mismatch" in str(e), f"Failed for protocol version check"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('set_external_index_response_status', 'crash_on_response_size_check', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index socket read failed" in str(e), f"Failed for set_external_index_response_status"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('set_external_index_response_status', 'crash_on_response_size_check', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"external index socket read failed" in str(e), f"Failed for set_external_index_response_status"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('BuildIndex', 'crash_after_recv_header', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"received invalid index header" in str(e), f"Failed for receive usearch header"

    try:
        primary.execute("testdb", f"""
                        SELECT _lantern_internal.failure_point_enable('external_index_receive_metadata', 'crash_on_end_msg', 0);
                        SET lantern.external_index_secure={use_ssl};
                        CREATE INDEX {index_name} ON {table_name} USING lantern_hnsw (v {ops}) WITH (dim=128, M=10, quant_bits = {quant_bits}, external = true);
        """)
        assert False
    except Exception as e:
        assert f"Resource temporarily unavailable" in str(e), f"Failed for crash_on_end_msg"

if __name__ == "__main__":
    os._exit(pytest.main(["-s", __file__, *sys.argv[1:]]))

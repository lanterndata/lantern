import testgres
import os

print("Starting replication/WAL tests...")

VECTOR_QUERY = "SELECT * FROM small_world WHERE b = FALSE order by v <-> '{1,0,0}' LIMIT 3;"

with testgres.get_new_node() as primary:

    # run inidb
    primary.init()

    primary.append_conf('enable_seqscan = off')

    primary.start()

    primary.execute('CREATE EXTENSION lantern')
    # testgres safe_psql does not support specifying cwd, so we have to change the cwd of the current
    # script to make sure relative paths in the target sql file work in testgres
    os.chdir('../test/sql')
    primary.safe_psql(filename='hnsw_delete.sql')
    # create a backup
    with primary.backup() as backup:

        # create and start a new replica
        replica = backup.spawn_replica('replica').start()

        # catch up with master node
        replica.catchup()

        # make sure we are using the index
        assert 'Index Scan using small_world_v_idx on small_world' in str(primary.safe_psql(f"EXPLAIN {VECTOR_QUERY}"))
        assert 'Index Scan using small_world_v_idx on small_world' in str(replica.safe_psql(f"EXPLAIN {VECTOR_QUERY}"))

        res = replica.execute(VECTOR_QUERY)
        assert res[0][2] == [1.0, 0.0, 0.0]
        # take only boolean columns
        assert [i[1] for i in res] == [False]

        # Now, let's delete data on primary and see how it propagates to replica
        primary.execute("INSERT INTO small_world (id, b, v) VALUES (42, FALSE, '{42,42,42}'), (43, FALSE, '{42,42,42}'), (44, FALSE, '{42,42,42}');")

        res = replica.execute(VECTOR_QUERY)
        # changes have not propagated yet
        assert res[0][2] == [1.0, 0.0, 0.0]
        assert [i[1] for i in res] == [False]
        replica.catchup()
        res = replica.execute(VECTOR_QUERY)
        assert res[0][2] == [1.0, 0.0, 0.0]
        assert [i[1] for i in res] == [False, False, False], 'failed %s' % res

print("WAL tests completed successfully!")


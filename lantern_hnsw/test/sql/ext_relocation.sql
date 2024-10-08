\ir utils/small_world_array.sql

DROP EXTENSION lantern;
\set ON_ERROR_STOP off
-- make sure the extension was dropped.
CREATE INDEX ON small_world USING lantern_hnsw (v) WITH (dim=3);
\set ON_ERROR_STOP on

-- test creating lantern on different schemas
CREATE SCHEMA schema1;
CREATE SCHEMA schema2;
CREATE EXTENSION lantern WITH SCHEMA schema1;

-- show all the extension functions and operators
SELECT ne.nspname AS extschema, p.proname, np.nspname AS proschema
FROM pg_catalog.pg_extension AS e
    INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
    INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
    INNER JOIN pg_catalog.pg_namespace AS ne ON (ne.oid = e.extnamespace)
    INNER JOIN pg_catalog.pg_namespace AS np ON (np.oid = p.pronamespace)
WHERE d.deptype = 'e' AND e.extname = 'lantern'
ORDER BY 1, 3, 2;

-- show all the extension operators
SELECT ne.nspname AS extschema, op.oprname, np.nspname AS proschema
FROM pg_catalog.pg_extension AS e
    INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
    INNER JOIN pg_catalog.pg_operator AS op ON (op.oid = d.objid)
    INNER JOIN pg_catalog.pg_namespace AS ne ON (ne.oid = e.extnamespace)
    INNER JOIN pg_catalog.pg_namespace AS np ON (np.oid = op.oprnamespace)
WHERE d.deptype = 'e' AND e.extname = 'lantern'
ORDER BY 1, 3;

SET search_path TO public, schema1;

-- extension function is accessible
SELECT l2sq_dist(ARRAY[1.0, 2.0, 3.0], ARRAY[4.0, 5.0, 6.0]);

CREATE INDEX hnsw_index ON small_world USING lantern_hnsw(v) WITH (dim=3);
SELECT _lantern_internal.validate_index('hnsw_index', false);

\set ON_ERROR_STOP off
-- lantern does not support relocation.
-- Postgres will not allow it to support this since its objects span over more than one schema
ALTER EXTENSION lantern SET SCHEMA schema2;
-- this will fail because functions from extension lantern in schema1 are in search path and will conflict
CREATE EXTENSION lantern WITH SCHEMA schema2;
\set ON_ERROR_STOP on

SELECT ne.nspname AS extschema, op.oprname, np.nspname AS proschema
FROM pg_catalog.pg_extension AS e
    INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
    INNER JOIN pg_catalog.pg_operator AS op ON (op.oid = d.objid)
    INNER JOIN pg_catalog.pg_namespace AS ne ON (ne.oid = e.extnamespace)
    INNER JOIN pg_catalog.pg_namespace AS np ON (np.oid = op.oprnamespace)
WHERE d.deptype = 'e' AND e.extname = 'lantern'
ORDER BY 1, 3;

SET search_path TO public, schema2;
--extension access method is still accessible since access methods are not schema-qualified
CREATE INDEX hnsw_index2 ON small_world USING lantern_hnsw(v) WITH (dim=3);
SELECT _lantern_internal.validate_index('hnsw_index2', false);

\set ON_ERROR_STOP off
-- extension function cannot be found without schema-qualification
SELECT l2sq_dist(ARRAY[1.0, 2.0, 3.0], ARRAY[4.0, 5.0, 6.0]);
\set ON_ERROR_STOP on
SELECT schema1.l2sq_dist(ARRAY[1.0, 2.0, 3.0], ARRAY[4.0, 5.0, 6.0]);

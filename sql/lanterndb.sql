-- Definitions concerning our hnsw-based index data strucuture

CREATE FUNCTION hnsw_handler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnsw_handler;

COMMENT ON ACCESS METHOD hnsw IS 'LanternDB vector index access method. Can be configured to use various strategies such hs hnsw, graph-based, disk-optimized etc.';


-- functions

CREATE FUNCTION l2sq_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


-- operators

CREATE OPERATOR <-> (
	LEFTARG = real[], RIGHTARG = real[], PROCEDURE = l2sq_dist,
	COMMUTATOR = '<->'
);


-- operator classes
CREATE OPERATOR CLASS ann_l2_ops
	DEFAULT FOR TYPE real[] USING hnsw AS
	OPERATOR 1 <-> (real[], real[]) FOR ORDER BY float_ops,
	FUNCTION 1 l2sq_dist(real[], real[]);

-- conditionaly create operator class for vector type
DO $$DECLARE type_exists boolean;
BEGIN
	-- Check if the vector type exists and store the result in the 'type_exists' variable
	SELECT EXISTS (
    	SELECT 1
    	FROM pg_type
    	WHERE typname = 'vector'
	) INTO type_exists;

	IF type_exists THEN
	-- The type exists
	-- taken from pgvector so our index can work with pgvector types
		CREATE OPERATOR CLASS vector_l2_ops
			DEFAULT FOR TYPE vector USING hnsw AS
			OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 vector_l2_squared_distance(vector, vector),
			FUNCTION 3 l2_distance(vector, vector);
	END IF;
END;
$$
LANGUAGE plpgsql;
-- -- CREATE TYPEs vec*

-- Function that are generic over the family of vec types

CREATE FUNCTION ldb_generic_vec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- 8-byte unit-vector (i.e. vector with elements in range [-1, 1])
CREATE TYPE uvec8;

CREATE FUNCTION ldb_uvec8_in(cstring, oid, integer) RETURNS uvec8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_uvec8_out(uvec8) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_uvec8_recv(internal, oid, integer) RETURNS uvec8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_uvec8_send(uvec8) RETURNS bytea AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE uvec8 (
	INPUT     = ldb_uvec8_in,
	OUTPUT    = ldb_uvec8_out,
	RECEIVE   = ldb_uvec8_recv,
	SEND      = ldb_uvec8_send,
	TYPMOD_IN = ldb_generic_vec_typmod_in,
	STORAGE   = extended
);

CREATE TYPE vec8;

CREATE FUNCTION ldb_vec8_in(cstring, oid, integer) RETURNS vec8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_vec8_out(vec8) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_vec8_recv(internal, oid, integer) RETURNS vec8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_vec8_send(vec8) RETURNS bytea AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE vec8 (
	INPUT     = ldb_vec8_in,
	OUTPUT    = ldb_vec8_out,
	RECEIVE   = ldb_vec8_recv,
	SEND      = ldb_vec8_send,
	TYPMOD_IN = ldb_generic_vec_typmod_in,
	STORAGE   = extended
);

-- cast functions

CREATE FUNCTION ldb_cast_uvec8_uvec8(uvec8, integer, boolean) RETURNS uvec8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_cast_array_uvec8(integer[], integer, boolean) RETURNS uvec8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_cast_array_uvec8(real[], integer, boolean) RETURNS uvec8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_cast_array_uvec8(double precision[], integer, boolean) RETURNS uvec8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_cast_array_uvec8(numeric[], integer, boolean) RETURNS uvec8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ldb_cast_vec_real(uvec8, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- casts

CREATE CAST (uvec8 AS uvec8)
	WITH FUNCTION ldb_cast_uvec8_uvec8(uvec8, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS uvec8)
	WITH FUNCTION ldb_cast_array_uvec8(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS uvec8)
	WITH FUNCTION ldb_cast_array_uvec8(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS uvec8)
	WITH FUNCTION ldb_cast_array_uvec8(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS uvec8)
	WITH FUNCTION ldb_cast_array_uvec8(numeric[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (uvec8 AS real[])
	WITH FUNCTION ldb_cast_vec_real(uvec8, integer, boolean) AS ASSIGNMENT;




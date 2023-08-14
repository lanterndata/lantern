-- functions
CREATE FUNCTION cos_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ham_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


-- operator classes
CREATE OPERATOR CLASS ann_cos_ops
FOR TYPE real[] USING hnsw AS
	FUNCTION 1 cos_dist(real[], real[]);

CREATE OPERATOR CLASS ann_ham_ops
	FOR TYPE real[] USING hnsw AS
	FUNCTION 1 ham_dist(real[], real[]);

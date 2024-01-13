CREATE FUNCTION lantern_reindex_external_index (index regclass) RETURNS VOID AS 'MODULE_PATHNAME',
'lantern_reindex_external_index' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

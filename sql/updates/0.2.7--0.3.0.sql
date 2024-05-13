WITH sub as (
  SELECT indrelid as table_oid, indexrelid, amname
  FROM pg_class t
  JOIN pg_index ix ON t.oid = ix.indrelid
  JOIN pg_class i ON i.oid = ix.indexrelid
  JOIN pg_am a ON i.relam = a.oid
  JOIN pg_namespace n ON n.oid = i.relnamespace
  WHERE a.amname = 'lantern_hnsw'
)
UPDATE pg_index ix
SET indisvalid = false, indisready = false
FROM sub
WHERE ix.indexrelid = sub.indexrelid;

DROP FUNCTION IF EXISTS _lantern_internal.continue_blockmap_group_initialization;

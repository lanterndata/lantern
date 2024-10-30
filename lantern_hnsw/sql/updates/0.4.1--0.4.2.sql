-- Get vector type oid
CREATE FUNCTION _lantern_internal.get_vector_type_oid() RETURNS OID AS $$
DECLARE
  type_oid OID;
BEGIN
  type_oid := (SELECT pg_type.oid FROM pg_type
                JOIN pg_depend ON pg_type.oid = pg_depend.objid
                JOIN pg_extension ON pg_depend.refobjid = pg_extension.oid 
                WHERE typname='vector' AND extname='vector'
                LIMIT 1);
  RETURN COALESCE(type_oid, 0);
END;
$$ LANGUAGE plpgsql;

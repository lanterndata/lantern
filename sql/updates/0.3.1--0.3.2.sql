CREATE OR REPLACE FUNCTION _lantern_internal.get_binary_version() RETURNS TEXT
	AS 'MODULE_PATHNAME', 'lantern_internal_get_binary_version' LANGUAGE C STABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _lantern_internal.get_catalog_version() RETURNS TEXT as $$
DECLARE
  cat_version TEXT;
BEGIN
   SELECT extversion INTO cat_version FROM pg_extension WHERE extname = 'lantern';
   RETURN cat_version;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _lantern_internal.parse_semver(semver text)
RETURNS TABLE(major int, minor int, patch int) AS $$
DECLARE
    parts TEXT[];
BEGIN
    IF semver = 'latest' THEN
        major := 2147483647;
        minor := 2147483647;
        patch := 2147483647;
    ELSE
        parts := string_to_array(semver, '.');

        major := COALESCE(parts[1], '0')::int;
        minor := COALESCE(parts[2], '0')::int;
        patch := COALESCE(parts[3], '0')::int;
    END IF;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- this function returns:
--    0  if versions are matching
--    1  if binary version is bigger  than catalog version
--    -1 if binary version is smaller than catalog version
CREATE OR REPLACE FUNCTION _lantern_internal.compare_extension_versions(catalog_version TEXT, binary_version TEXT) RETURNS INT as $$
DECLARE
    major1 INT;
    minor1 INT;
    patch1 INT;
    major2 INT;
    minor2 INT;
    patch2 INT;
    sum1 INT;
    sum2 INT;
BEGIN
 -- binary version
 SELECT major, minor, patch
    INTO major1, minor1, patch1
    FROM _lantern_internal.parse_semver(binary_version);

 -- sql version
 SELECT major, minor, patch
    INTO major2, minor2, patch2
    FROM _lantern_internal.parse_semver(catalog_version);

 -- Compare the major versions
 IF major1 < major2 THEN
     RETURN -1;
 ELSIF major1 > major2 THEN
     RETURN 1;
 END IF;

 -- Compare the minor versions
 IF minor1 < minor2 THEN
     RETURN -1;
 ELSIF minor1 > minor2 THEN
     RETURN 1;
 END IF;

 -- Compare the patch versions
 IF patch1 < patch2 THEN
     RETURN -1;
 ELSIF patch1 > patch2 THEN
     RETURN 1;
 ELSE
     RETURN 0;
 END IF;
END $$ LANGUAGE plpgsql;

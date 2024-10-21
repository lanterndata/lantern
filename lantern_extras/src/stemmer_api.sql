CREATE FUNCTION lantern_extras.ts_vector_to_ordered_string(tsvec tsvector)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $function$
DECLARE
    result text := '';
    lexeme_record RECORD;
BEGIN

    FOR lexeme_record IN
        SELECT
            unnest(string_to_array((regexp_matches(token, ':(\d+(,\d+)*)$'))[1], ','))::integer AS position,
            (regexp_matches(token, '(\w+)'))[1] AS lexeme
        FROM (
            SELECT
                unnest(string_to_array(tsvec::text, ' ')) AS token
        ) AS unnested_tokens
        ORDER BY
            position
    LOOP
        -- raise notice 'lexeme: %, position: %', lexeme_record.lexeme, lexeme_record.position;
        result := result || lexeme_record.lexeme || ' ';
    END LOOP;

    -- remove trailing space
    result := rtrim(result);

    RETURN result;
END;
$function$;

CREATE FUNCTION text_to_stem_array_tsvector(query TEXT, tsvector_strategy REGCONFIG = 'english')
RETURNS TEXT[]
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $function$
DECLARE
    result text[];
BEGIN
    query := regexp_replace(query, '([+\-!(){}\[\]^"~*?:\\<])', ' ', 'g');
    result := string_to_array(
                    lantern_extras.ts_vector_to_ordered_string(
                        to_tsvector(tsvector_strategy, query)
                    ),' '
                );
    RETURN result;
END;
$function$;


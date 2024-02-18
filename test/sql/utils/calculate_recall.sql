CREATE OR REPLACE FUNCTION calculate_table_recall(tbl regclass, query_tbl regclass, truth_tbl regclass, col NAME, k INT, cnt INT)
RETURNS FLOAT
AS $$
DECLARE
stmt TEXT;
result FLOAT;
BEGIN
    stmt := format('
        SELECT ROUND(AVG(r.q_recall)::numeric, 2) FROM (WITH q AS (
            SELECT
                id,
                v
            FROM
                %2$I
            LIMIT
                %6$s
        )
        SELECT 
            ARRAY_LENGTH(
            ARRAY(
                SELECT UNNEST(array_agg(b.id))
                INTERSECT
                SELECT UNNEST(t.indices[1:%5$s])
            ), 1)::FLOAT / %5$s::FLOAT as q_recall
        FROM q
        JOIN LATERAL (
            SELECT
                id
            FROM
                %1$I
            ORDER BY
                %1$I.%4$I <=> q.v
            LIMIT
                %5$s
        ) b ON TRUE
        LEFT JOIN
            %3$I AS t
        ON
            t.id = q.id
        GROUP BY
            q.id,
            t.indices) r;
    ', tbl, query_tbl, truth_tbl, col, k, cnt);

     EXECUTE stmt INTO result;
     RETURN result;
END;
$$ LANGUAGE plpgsql;

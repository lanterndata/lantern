CREATE OR REPLACE FUNCTION random_int_array(dim integer, min integer, max integer) RETURNS integer[] AS $BODY$
begin
        return (select array_agg(round(random() * (max - min)) + min) from generate_series (0, dim - 1));
end
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION random_array(dim integer, min real, max real) RETURNS REAL[] AS $BODY$
begin
        return (select array_agg(random() * (max - min) + min) from generate_series (0, dim - 1));
end
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION random_int_array(dim integer, min integer, max integer) RETURNS integer[] AS $BODY$
begin
	return (select array_agg(round(random() * (max - min)) + min) from generate_series (0, dim));
end
$BODY$ LANGUAGE plpgsql;

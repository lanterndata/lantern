import psycopg2

# Database connection parameters
db_params = {
    'database': 'postgres',
    'user': 'postgres',  # Update with your username if different
    'password': '',  # Update with your password if required
    'host': 'localhost',
    'port': '5432'
}

# Connect to the database
conn = psycopg2.connect(**db_params)
conn.autocommit = True
cur = conn.cursor()

# Execute the SQL commands
cur.execute("""
DROP EXTENSION IF EXISTS lantern;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lantern;

CREATE TABLE IF NOT EXISTS small_world_weighted_search (
    id VARCHAR(3) PRIMARY KEY,
    b BOOLEAN,
    v VECTOR(3),
    s SPARSEVEC(3)
);

INSERT INTO small_world_weighted_search (id, b, v, s) VALUES
    ('000', TRUE,  '[0,0,0]', '{}/3'),
    ('001', TRUE,  '[0,0,1]', '{3:1}/3'),
    ('010', FALSE, '[0,1,0]' , '{2:1}/3'),
    ('011', TRUE,  '[0,1,1]', '{2:1,3:1}/3'),
    ('100', FALSE, '[1,0,0]', '{1:1}/3'),
    ('101', FALSE, '[1,0,1]', '{1:1,3:1}/3'),
    ('110', FALSE, '[1,1,0]', '{1:1,2:1}/3'),
    ('111', TRUE,  '[1,1,1]', '{1:1,2:1,3:1}/3')
ON CONFLICT DO NOTHING;
""")

distance_metrics = ["", "cos", "l2sq"]
for distance_metric in distance_metrics:
    operator = op = { 'l2sq': '<->', 'cos': '<=>', 'hamming': '<+>' }[distance_metric or 'l2sq']
    query_s = "{1:0.4,2:0.3,3:0.2}/3"
    query_v = "[-0.5,-0.1,-0.3]"
    function = f'weighted_vector_search_{distance_metric}' if distance_metric else 'weighted_vector_search'
    query = f"""
        SELECT
            id,
            round(cast(0.9 * (s {operator} '{query_s}'::sparsevec) + 0.1 * (v {operator} '{query_v}'::vector) as numeric), 2) as dist
        FROM lantern.{function}(CAST(NULL as "small_world_weighted_search"), distance_operator=>'{operator}',
            w1=> 0.9, col1=>'s'::text, vec1=>'{query_s}'::sparsevec,
            w2=> 0.1, col2=>'v'::text, vec2=>'{query_v}'::vector
        )
        LIMIT 3;
    """
    cur.execute(query)
    res = cur.fetchall()
    res = [(key, float(value)) for key, value in res]

    expected_results_cos = [('111', 0.22), ('110', 0.24), ('101', 0.39)]
    expected_results_l2sq = [('000', 0.54), ('100', 0.78), ('010', 0.87)]
    if distance_metric == 'cos':
        assert res == expected_results_cos
    else:
        assert res == expected_results_l2sq
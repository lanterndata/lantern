import os
import subprocess
import psycopg2
from psycopg2 import sql

# Ensure LANTERN_CLI environment variable is set
lantern_cli = os.getenv('LANTERN_CLI')
if lantern_cli is None:
    raise EnvironmentError("LANTERN_CLI environment variable is not set.")

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

# Drop the ldb_test_lantern_cli database if it exists and recreate it
cur.execute("DROP DATABASE IF EXISTS ldb_test_lantern_cli;")
cur.execute("CREATE DATABASE ldb_test_lantern_cli;")

# Close connection to the default database and connect to the new database
cur.close()
conn.close()

db_params['database'] = 'ldb_test_lantern_cli'
conn = psycopg2.connect(**db_params)
conn.autocommit = True
cur = conn.cursor()

# Create the table
cur.execute("""
CREATE TABLE sift_base10k (
    id SERIAL PRIMARY KEY,
    v REAL[128]
);
""")

# Import data from CSV
cur.execute(r"""
COPY sift_base10k (v) FROM '/tmp/lantern/vector_datasets/siftsmall_base_arrays.csv' WITH CSV;
""")

conn.commit()

# Run the LANTERN_CLI create-index command
subprocess.run([lantern_cli, 'create-index', '--uri', 'postgresql://localhost:5432/ldb_test_lantern_cli', '--table', 'sift_base10k', '--column', 'v'], check=True)

# Create the hnsw_l2_index
cur.execute("""
CREATE EXTENSION IF NOT EXISTS lantern;
CREATE INDEX hnsw_l2_index ON sift_base10k USING hnsw(v) WITH (_experimental_index_path='/lantern_shared/build/index.usearch');
""")

# Validate the index
cur.execute("SELECT _lantern_internal.validate_index('hnsw_l2_index', false);")

# Fetch and print results (for demonstration purposes)
print(cur.fetchall())

# Cleanup
cur.close()
conn.close()


# LanternDB 🏮

[![build](https://github.com/lanterndata/lanterndb/actions/workflows/build-linux.yaml/badge.svg?branch=main)](https://github.com/lanterndata/lanterndb/actions/workflows/build-linux.yaml)
[![test](https://github.com/lanterndata/lanterndb/actions/workflows/test-linux.yaml/badge.svg?branch=main)](https://github.com/lanterndata/lanterndb/actions/workflows/test-linux.yaml)
[![codecov](https://codecov.io/github/lanterndata/lanterndb/branch/main/graph/badge.svg)](https://codecov.io/github/lanterndata/lanterndb)

LanternDB is a relational and vector database, packaged as a Postgres extension.
It provides a new index type for vector columns called `hnsw` which speeds up `ORDER BY` queries on the table.

## Quickstart

### Building and Installing LanternDB

LanternDB builds and uses [usearch](https://github.com/unum-cloud/usearch) for its single-header state-of-the-art HNSW implementation.

To build and install LanternDB:

```bash
git clone --recursive https://github.com/lanterndata/lanterndb.git
cd lanterndb
mkdir build
cd build
cmake ..
make install
# optionally
# make test
```

<details>
<summary>If you have previously cloned LanternDB and would like to update</summary>
```bash
git pull
git submodule update
```
</details>

To install on M1 macs, replace `cmake ..` from the above with `cmake -DUSEARCH_NO_MARCH_NATIVE=ON ..` to avoid building usearch with unsupported `march=native`

## Using LanternDB

1. Run the following SQL command to enable lanterndb:

```sql
CREATE EXTENSION lanterndb;
```

2. Create a table with a vector column and populate it with data.

```sql
CREATE TABLE small_world (
    id varchar(3),
    vector real[]
);

INSERT INTO small_world (id, vector) VALUES
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');
```

3. Create an `hnsw` index on the table.

```sql
-- create index with default parameters
CREATE INDEX ON small_world USING hnsw (vector);
-- create index with custom parameters
-- CREATE INDEX ON small_world USING hnsw (vector) WITH (M=2, ef_construction=10, ef=4, dim=3);
```

4. Leverage the index in queries like:

```sql
SELECT id, ROUND(l2sq_dist(vector, array[0,0,0])::numeric, 2) as dist
FROM small_world
ORDER BY vector <-> array[0,0,0] LIMIT 5;
```

### A Note on Index Construction Parameters

The `M`, `ef`, and `efConstruction` parameters control the tradeoffs of the HNSW algorithm.
In general, lower `M` and `efConstruction` speed up index creation at the cost of recall.
Lower `M` and `ef` improve search speed and result in fewer shared buffer hits at the cost of recall.
Tuning these parameters will require experimentation for your specific use case. An upcoming LanternDB release will include an optional auto-tuning index.

### A Note on Performance

LanternDB's `hnsw` enables search latency similar to pgvector's `ivfflat` and is faster than `ivfflat` under certain construction parameters. LanternDB enables higher search throughput on the same hardware since the HNSW algorithm requires fewer distance comparisons than the IVF algorithm, leading to less CPU usage per search.

### A note on operators and operator classes

Currently, there is only one operator `<->` available.  
This operator is intended exclusively for use with index lookups, such as in cases like `ORDER BY vector <-> array[0,0,0]`.  
Consequently, attempting to execute the query `SELECT array[0,0,0] <-> array[0,0,0]` will result in an error.

There are four defined operator classes that can be employed during index creation:

- **dist_l2sq_ops:** Default for the type `real[]`
- **dist_vec_l2sq_ops:** Default for the type `vector`
- **dist_cos_ops:** Applicable to the type `real[]`
- **dist_hamming_ops:** Applicable for the type `integer[]`

When creating an index, you have the option to specify the operator class to be used, like so:

```sql
CREATE INDEX ON small_world USING hnsw (vector dist_cos_ops);
```

This approach allows the `<->` operator to automatically identify the appropriate distance function when utilized in index lookups.

# Roadmap

- [x] Postgres wal-backed hnsw index creation on existing tables with sane defaults
- [x] Efficient index lookups, backed by usearch and postgres wal
- [ ] `INSERT`s into the created index
- [ ] `DELETE`s from the index and `VACUUM`ing
- [ ] Automatic index creation parameter (`M`, `ef`, `efConstruction`) tuning
- [ ] Support for 16bit and 8bit vector elements
- [ ] Support for over 2000 dimensional vectors
- [ ] Support for `INDEX-ONLY` scans
- [ ] Support for `INCLUDE` clauses in index creation, to expand the use of `INDEX-ONLY` scans
- [ ] Allow out-of-band indexing and external index importing (to speed up index generation for large tables)
- [ ] Allow using postgres `ARRAY`s as vectors
- [ ] Add more distance functions
- [ ] Add Product Quantization as another vector compression method
- [ ] Implement a Vamana index introduced in [DiskANN](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf) to potentially reduce the number of buffers hit during an index scan.

# 💡 Lantern

[![build](https://github.com/lanterndata/lanterndb/actions/workflows/build-linux.yaml/badge.svg?branch=main)](https://github.com/lanterndata/lanterndb/actions/workflows/build-linux.yaml)
[![test](https://github.com/lanterndata/lanterndb/actions/workflows/test-linux.yaml/badge.svg?branch=main)](https://github.com/lanterndata/lanterndb/actions/workflows/test-linux.yaml)
[![codecov](https://codecov.io/github/lanterndata/lanterndb/branch/main/graph/badge.svg)](https://codecov.io/github/lanterndata/lanterndb)
[![Run on Replit](https://img.shields.io/badge/Run%20on-Replit-blue?logo=replit)](https://replit.com/@lanterndata/lanterndb-playground#.replit)

Lantern is an open-source PostgreSQL database extension to store vector data, generate embeddings, and handle vector search operations.

It provides a new index type for vector columns called `hnsw` which speeds up `ORDER BY ... LIMIT` queries.

Lantern builds and uses [usearch](https://github.com/unum-cloud/usearch), a single-header state-of-the-art HNSW implementation.

## 🔧 Quick Install

If you don’t have PostgreSQL already, use Lantern with [Docker](https://hub.docker.com/r/lanterndata/lanterndb
) to get started quickly:
```bash
docker run -it -p 5432:5432 lanterndata/lanterndb
```

To install Lantern on top of PostgreSQL:
```
git clone --recursive https://github.com/lanterndata/lanterndb.git
cd lanterndb
mkdir build
cd build
cmake ..
make install
```
You can also install Lantern on top of PostgreSQL from our [precompiled binaries](https://github.com/lanterndata/lanterndb/releases) via a single make install

## 📖 How to use Lantern 

Lantern retains the standard PostgreSQL interface, so it is compatible with all of your favorite tools in the PostgreSQL ecosystem.

First, enable Lantern in SQL

```sql
CREATE EXTENSION lanterndb;
```

Create a table with a vector column and add your data

```sql
CREATE TABLE small_world (id integer, vector real[3]);
INSERT INTO small_world (id, vector) VALUES (0, '{0,0,0}'), (1, '{0,0,1}');
```

Create an `hnsw` index on the table

```sql
CREATE INDEX ON small_world USING hnsw (vector);
```

Customize `hnsw` index parameters depending on your vector data, such as the distance function (e.g., `dist_l2sq_ops`), index construction parameters, and index search parameters.
```sql
CREATE INDEX ON small_world USING hnsw (vector dist_l2sq_ops)
WITH (M=2, ef_construction=10, ef=4, dims=3);
```

Start querying data
```sql
SELECT id, l2sq_dist(vector, ARRAY[0,0,0]) AS dist
FROM small_world ORDER BY vector <-> ARRAY[0,0,0] LIMIT 1;
```

### A note on operators and operator classes

Lantern supports several distance functions in the index. You only need to specify the distance function used for a column at index creation time. Lantern will automatically infer the distance function to use for search so you always use `<->` operator in search queries.

Note that the operator `<->` is intended exclusively for use with index lookups. If you expect to not use the index in a query, just use the distance function directly (e.g. `l2sq_dist(v1, v2)`)

There are four defined operator classes that can be employed during index creation:

- **`dist_l2sq_ops`**: Default for the type `real[]`
- **`dist_vec_l2sq_ops`**: Default for the type `vector`
- **`dist_cos_ops`**: Applicable to the type `real[]`
- **`dist_hamming_ops`**: Applicable for the type `integer[]`

### Index Construction Parameters

The `M`, `ef`, and `ef_construction` parameters control the performance of the HNSW algorithm for your use case. 
- In general, lower `M` and `ef_construction` speed up index creation at the cost of recall. 
- Lower `M` and `ef` improve search speed and result in fewer shared buffer hits at the cost of recall. Tuning these parameters will require experimentation for your specific use case.

### Miscellaneous
- If you have previously cloned Lantern and would like to update run `git pull && git submodule update`

## ⭐️ Features 
- Embedding generation for popular use cases (CLIP model, Hugging Face models, custom model)
- Interoperability with pgvector's data type, so anyone using pgvector can switch to Lantern 
- Parallel index creation capabilities (up to 40x faster than constructors like pgvector + pgembedding)
- Support for creating the index outside of the database and inside another instance allows you to create an index without interrupting database workflows.
- See all of our helper functions to better enable your workflows 

## 🏎️ Performance

Important takeaways: 
- Lantern is already 40x faster than pgvector and Neon’s pgembedding at creating an index to store data. 
- We match and often outperform pgvector and Neon’s pgembedding on latency + throughput. 

Graph 1 — Throughput

Graph 2 — Latency 

Graph 3 — Index Creation

## 🗺️ Roadmap

- Cloud-hosted version of Lantern
- Hardware-accelerated distance metrics, tailored for your CPU, enabling faster queries
- Templates and guides for building applications for different industries
- More tools for generating embeddings (support for third party model API’s, more local models) 
- Support for version control and A/B test embeddings
- Autotuned index type that will choose appropriate  creation parameters
- [Support](https://github.com/lanterndata/lanterndb/pull/19) for 1 byte and 2 byte vector elements, and up to 8000 dimensional vectors
- Request a feature at [support@lantern.dev](mailto:support@lantern.dev)

## 📚 Resources

- [GitHub issues](https://github.com/lanterndata/lanterndb/issues): report bugs or issues with Lantern
- Need support? Contact [support@lantern.dev](mailto:support@lantern.dev). We are happy to troubleshoot issues and advise on how to use Lantern for your use case 
- We welcome community contributions! Feel free to open an issue or a PR. If you contact [support@lantern.dev](mailto:support@lantern.dev), we can find an open issue or project that fits you
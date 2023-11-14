# 💡 Lantern

[![build](https://github.com/lanterndata/lantern/actions/workflows/build.yaml/badge.svg?branch=main)](https://github.com/lanterndata/lantern/actions/workflows/build.yaml)
[![test](https://github.com/lanterndata/lantern/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/lanterndata/lantern/actions/workflows/test.yaml)
[![codecov](https://codecov.io/github/lanterndata/lantern/branch/main/graph/badge.svg)](https://codecov.io/github/lanterndata/lantern)
[![Run on Replit](https://img.shields.io/badge/Run%20on-Replit-blue?logo=replit)](https://replit.com/@lanterndata/lantern-playground#.replit)

Lantern is an open-source PostgreSQL database extension to store vector data, generate embeddings, and handle vector search operations.

It provides a new index type for vector columns called `hnsw` which speeds up `ORDER BY ... LIMIT` queries.

Lantern builds and uses [usearch](https://github.com/unum-cloud/usearch), a single-header state-of-the-art HNSW implementation.

## 🔧 Quick Install

If you don’t have PostgreSQL already, use Lantern with [Docker](https://hub.docker.com/r/lanterndata/lantern) to get started quickly:

```bash
docker run -p 5432:5432 -e 'POSTGRES_PASSWORD=postgres' lanterndata/lantern:latest-pg15
```

To install Lantern from source on top of PostgreSQL:

```
git clone --recursive https://github.com/lanterndata/lantern.git
cd lantern
mkdir build
cd build
cmake ..
make install
```

To install Lantern using `homebrew`:

```
brew tap lanterndata/lantern
brew install lantern && lantern_install
```

You can also install Lantern on top of PostgreSQL from our [precompiled binaries](https://github.com/lanterndata/lantern/releases) via a single `make install`.

Alternatively, you can use Lantern in one click using [Replit](https://replit.com/@lanterndata/lantern-playground#.replit).

## 📖 How to use Lantern

Lantern retains the standard PostgreSQL interface, so it is compatible with all of your favorite tools in the PostgreSQL ecosystem.

First, enable Lantern in SQL

```sql
CREATE EXTENSION lantern;
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
WITH (M=2, ef_construction=10, ef=4, dim=3);
```

Start querying data

```sql
SET enable_seqscan = false;
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
- Parallel index creation via an external indexer
- Ability to generate the index graph outside of the database server
- Support for creating the index outside of the database and inside another instance allows you to create an index without interrupting database workflows.
- See all of our helper functions to better enable your workflows

## 🏎️ Performance

Important takeaways:

- There's three key metrics we track. `CREATE INDEX` time, `SELECT` throughput, and `SELECT` latency.
- We match or outperform pgvector and pg_embedding (Neon) on all of these metrics.
- We plan to continue to make performance improvements to ensure we are the best performing database.

<p>
<img alt="Lantern throughput" src="https://storage.googleapis.com/lantern-blog/1/throughput.png" width="400" style="float: left;" />
<img alt="Lantern latency" src="https://storage.googleapis.com/lantern-blog/1/latency.png" width="400" style="float: left;" />
<img alt="Lantern index creation" src="https://storage.googleapis.com/lantern-blog/1/create.png" width="400" style="float: left;" />
</p>

## 🗺️ Roadmap

- Cloud-hosted version of Lantern - [Sign up](https://forms.gle/YwxTzN9138LZEeCw8) for updates
- Hardware-accelerated distance metrics, tailored for your CPU, enabling faster queries
- Templates and guides for building applications for different industries
- More tools for generating embeddings (support for third party model API’s, more local models)
- Support for version control and A/B test embeddings
- Autotuned index type that will choose appropriate creation parameters
- Support for 1 byte and 2 byte vector elements, and up to 8000 dimensional vectors ([PR #19](https://github.com/lanterndata/lantern/pull/19))
- Request a feature at [support@lantern.dev](mailto:support@lantern.dev)

## 📚 Resources

- [GitHub issues](https://github.com/lanterndata/lantern/issues): report bugs or issues with Lantern
- Need support? Contact [support@lantern.dev](mailto:support@lantern.dev). We are happy to troubleshoot issues and advise on how to use Lantern for your use case
- We welcome community contributions! Feel free to open an issue or a PR. If you contact [support@lantern.dev](mailto:support@lantern.dev), we can find an open issue or project that fits you

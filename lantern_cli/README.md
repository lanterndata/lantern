# Lantern CLI

## Lantern Index Builder

## Description

This is a CLI application that creates an index for Lantern outside of Postgres which can later be imported into Postgres. This allows for faster index creation through parallelization.

## How to use

### Installation

Run `cargo install --path lantern_cli` to install the binary

### Usage

Run `lantern-cli create-index --help` to show the cli options.

```bash
Usage: lantern-cli create-index --uri <URI> --table <TABLE> --column <COLUMN> -m <M> --efc <EFC> --ef <EF> -d <DIMS> --metric-kind <METRIC_KIND> --out <OUT> --import
```

### Example

```bash
lantern-cli create-index -u "postgresql://localhost/test" -t "small_world" -c "vec" -m 16 --ef 64 --efc 128 -d 3 --metric-kind cos --out /tmp/index.usearch --import
```

### Notes

The index should be created from the same database on which it will be loaded, so row tids will match later.

## Lantern Embeddings

## Description

This is a CLI application that generates vector embeddings from your postgres data.

## How to use

### Installation

Run `cargo install --path lantern_cli` to install the binary if you have clonned the source code or `cargo install --git https://github.com/lanterndata/lantern.git` to install from git.

or build and use the docker image

```bash
# Run with CPU version
docker run -v models-volume:/models --rm --network host lanterndata/lantern-cli create-embeddings --model 'BAAI/bge-large-en' --uri 'postgresql://postgres@host.docker.internal:5432/postgres' --table "wiki" --column "content" --out-column "content_embedding" --batch-size 40 --data-path /models

# Run with GPU verion
nvidia-docker run -v models-volume:/models --rm --network host lanterndata/lantern-cli:gpu create-embeddings  --model 'BAAI/bge-large-en' --uri 'postgresql://postgres@host.docker.internal:5432/postgres' --table "wiki" --column "content" --out-column "content_embedding" --batch-size 40 --data-path /models
```

> [nvidia-container-runtime](https://developer.nvidia.com/nvidia-container-runtime) is required for GPU version to work. You can check the GPU load using `nvtop` command (`apt install nvtop`)

### Usage

Run `lantern-cli create-embeddings --help` to show the cli options.
Run `lantern-cli show-models` to show available models.

### Text Embedding Example

1. Create table with text data

```sql
CREATE TABLE articles (id SERIAL, description TEXT, embedding REAL[]);
INSERT INTO articles SELECT generate_series(0,999), 'My description column!';
```

> Currently it is requried for table to have id column, so it could map the embedding with row when exporting output.

2. Run embedding generation

```bash
lantern-cli create-embeddings  --model 'clip/ViT-B-32-textual'  --uri 'postgresql://postgres:postgres@localhost:5432/test' --table "articles" --column "description" --out-column "embedding" --schema "public"
```

> The output database, table and column names can be specified via `--out-table`, `--out-uri`, `--out-column` arguments. Check `help` for more info.

or you can export to csv file

```bash
lantern-cli create-embeddings  --model 'clip/ViT-B-32-textual'  --uri 'postgresql://postgres:postgres@localhost:5432/test' --table "articles" --column "description" --out-column embedding --out-csv "embeddings.csv" --schema "public"
```

### Image Embedding Example

1. Create table with image uris data

```sql
CREATE TABLE images (id SERIAL, url TEXT, embedding REAL[]);
INSERT INTO images (url) VALUES ('https://cdn.pixabay.com/photo/2014/11/30/14/11/cat-551554_1280.jpg'), ('https://cdn.pixabay.com/photo/2016/12/13/05/15/puppy-1903313_1280.jpg');
```

2. Run embedding generation

```bash
lantern-cli create-embeddings  --model 'clip/ViT-B-32-visual'  --uri 'postgresql://postgres:postgres@localhost:5432/test' --table "images" --column "url" --out-column "embedding" --schema "public" --visual
```

### OpenAI and Cohere Embeddings

Lantern CLI also supports generating OpenAI and Cohere embeddings via API. For that you should specify `--runtime` and `--runtime-params` arguments

```bash
# OpenAI
lantern-cli create-embeddings  --model 'openai/text-embedding-ada-002' --uri 'postgresql://postgres:postgres@localhost:5432/test' --table "images" --column "url" --out-column "embedding" --schema "public" --runtime openai --runtime-params '{ "api_token": "sk-xxx-xxxx" }'

# Cohere
lantern-cli create-embeddings  --model 'openai/text-embedding-ada-002' --uri 'postgresql://postgres:postgres@localhost:5432/test' --table "images" --column "url" --out-column "embedding" --schema "public" --runtime cohere --runtime-params '{ "api_token": "xxx-xxxx" }'
```

|> To get available runtimes use `bash lantern-cli show-runtimes`

### Index Autotune

Lantern CLI supports autotuning HNSW index parameters. To use the functionality run

```bash
lantern-cli autotune-index -u 'postgresql://postgres:postgres@localhost:5432/test' -t "sift1m" -c "v" --metric-kind l2sq --test-data-size 10000 --k 20
```

To get full list of arguments use `bash lantern-cli autotune-index -h`

### Daemon Mode

Lantern CLI can be used in daemon mode to continousely listen to postgres table and generate embeddings, external indexes or autotune jobs.

```bash
 lantern-cli start-daemon --uri 'postgres://postgres@localhost:5432/postgres' --embedding-table embedding_jobs --autotune-table index_autotune_jobs --autotune-results-table index_parameter_experiment_results --external-index-table external_index_jobs --schema public --log-level debug
```

This will set up trigger on specified table (`lantern_jobs`) and when new row will be inserted it will start embedding generation based on row data.
After that the triggers will be set up in target table, so it will generate embeddings continousely for that table.
The jobs table should have the following structure

```sql
-- Embedding Jobs Table should have the following structure:
CREATE TABLE "public"."embedding_jobs" (
    "id" SERIAL PRIMARY KEY,
    "database_id" text NOT NULL,
    "db_connection" text NOT NULL,
    "schema" text NOT NULL,
    "table" text NOT NULL,
    "runtime" text NOT NULL,
    "runtime_params" jsonb,
    "src_column" text NOT NULL,
    "dst_column" text NOT NULL,
    "embedding_model" text NOT NULL,
    "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "canceled_at" timestamp,
    "init_started_at" timestamp,
    "init_finished_at" timestamp,
    "init_failed_at" timestamp,
    "init_failure_reason" text,
    "init_progress" int2 DEFAULT 0
);
-- External Index Jobs Table should have the following structure:
CREATE TABLE "public"."external_index_jobs" (
    "id" SERIAL PRIMARY KEY,
    "database_id" text NOT NULL,
    "db_connection" text NOT NULL,
    "schema" text NOT NULL,
    "table" text NOT NULL,
    "column" text NOT NULL,
    "index" text,
    "operator" text NOT NULL,
    "efc" INT NOT NULL,
    "ef" INT NOT NULL,
    "m" INT NOT NULL,
    "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "canceled_at" timestamp,
    "started_at" timestamp,
    "finished_at" timestamp,
    "failed_at" timestamp,
    "failure_reason" text,
    "progress" INT2 DEFAULT 0
);
-- Autotune Jobs Table should have the following structure:
CREATE TABLE "public"."index_autotune_jobs" (
    "id" SERIAL PRIMARY KEY,
    "database_id" text NOT NULL,
    "db_connection" text NOT NULL,
    "schema" text NOT NULL,
    "table" text NOT NULL,
    "column" text NOT NULL,
    "operator" text NOT NULL,
    "target_recall" DOUBLE PRECISION NOT NULL,
    "embedding_model" text NULL,
    "k" int NOT NULL,
    "n" int NOT NULL,
    "create_index" bool NOT NULL,
    "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "canceled_at" timestamp,
    "started_at" timestamp,
    "progress" INT2 DEFAULT 0,
    "finished_at" timestamp,
    "failed_at" timestamp,
    "failure_reason" text
);

-- Autotune results table should have the following structure:
CREATE TABLE "public"."index_parameter_experiment_results" (
     id SERIAL PRIMARY KEY,
     experiment_id INT NOT NULL, -- reference to job.id
     ef INT NOT NULL,
     efc INT  NOT NULL,
     m INT  NOT NULL,
     recall DOUBLE PRECISION NOT NULL,
     latency DOUBLE PRECISION NOT NULL,
     build_time DOUBLE PRECISION NULL
);
```

## Lantern PQ

## Description

Use external product quantization to compress table vectors using kmeans clustering.

### Usage

Run `lantern-cli pq-table --help` to show the cli options.

Job can be run both on local instance and also using GCP batch jobs to parallelize the workload over handreds of VMs to speed up clustering.

To run locally use:

```bash
lantern-cli pq-table --uri 'postgres://postgres@127.0.0.1:5432/postgres' --table sift10k --column v --clusters 256 --splits 32
```

The job will be run on current machine utilizing all available cores.

For big datasets over 1M it is convinient to run the job using GCP batch jobs.  
Make sure to have GCP credentials set-up before running this command:

```bash
lantern-cli pq-table --uri 'postgres://postgres@127.0.0.1:5432/postgres' --table sift10k --column v --clusters 256 --splits 32 --run-on-gcp
```

If you prefer to orchestrate task on your own on premise servers you need to do the following 3 steps:

1. Run setup job. This will create necessary tables and add `pqvec` column on target table

```bash
lantern-cli pq-table --uri 'postgres://postgres@127.0.0.1:5432/postgres' --table sift10k --column v --clusters 256 --splits 32 --skip-codebook-creation --skip-vector-compression
```

2. Run clustering job. This will create codebook for the table and export to postgres table

```bash
lantern-cli pq-table --uri 'postgres://postgres@127.0.0.1:5432/postgres' --table sift10k --column v --clusters 256 --splits 32 --skip-table-setup --skip-vector-compression --parallel-task-count 10 --subvector-id 0
```

In this case this command should be run 32 times for each subvector in range [0-31] and `--parallel-task-count` means at most we will run 10 tasks in parallel. This is used to not exceed max connection limit on postgres.

3. Run compression job. This will compress vectors using the generated codebook and export results under `pqvec` column

```bash
lantern-cli pq-table --uri 'postgres://postgres@127.0.0.1:5432/postgres' --table sift10k --column v --clusters 256 --splits 32 --skip-table-setup --skip-codebook-creation --parallel-task-count 10 --total-task-count 10 --compression-task-id 0
```

In this case this command should be run 10 times for each part of codebook in range [0-9] and `--parallel-task-count` means at most we will run 10 tasks in parallel. This is used to not exceed max connection limit on postgres.

Table should have primary key, in order for this job to work. If primary key is different than `id` provide it using `--pk` argument


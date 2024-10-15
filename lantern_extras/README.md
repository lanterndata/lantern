# Lantern Extras

This extension makes it easy to experiment with embeddings from inside a Postgres database. We use this extension along with [Lantern](https://github.com/lanterndata/lantern) to make vector operations performant. But all the helpers here are standalone and may be used without the main database.

**NOTE**: Functions defined in this extension use Postgres in ways Postgres is usually not used.
Some calls may result in large file downloads, or CPU-intensive model inference operations. Keep this in mind when using this extension a shared Postgres environment.

## Features

- Streaming download of vector embeddings in archived and uncompressed formats
- Streaming download of various standard vector benchmark datasets
  - SIFT
  - GIST
- Generation of various various embeddings for data stored in Postgres tables without leaving the database

## Examples

```sql
-- parse the first 41 vectors from the uncompressed .fvecs vector dataset on server machine
SELECT parse_fvecs('/tmp/rustftp/siftsmall/siftsmall_base.fvecs', 41);

-- load the first 10k vectors from the uncompressed vector dataset into a table named sift
SELECT * INTO sift FROM parse_fvecs('/tmp/rustftp/siftsmall/siftsmall_base.fvecs', 10000);

-- load SIFT dataset ground truth vectors into a table from an online ftp archive
SELECT query,
       true_nearest INTO sift_ground
FROM get_sift_groundtruth('ftp://host/path/to/siftsmall.tar.gz');

-- generate CLIP embeddings for columns of a postgres table
SELECT abstract,
       introduction,
       figure1,
       clip_text(abstract) AS abstract_ai,
       clip_text(introduction) AS introduction_ai,
       clip_image(figure1) AS figure1_ai
INTO papers_augmented
FROM papers;

```

-- generate embeddings from other models which can be extended

```sql
-- generate text embedding
SELECT text_embedding('BAAI/bge-base-en', 'My text input');
-- generate image embedding with image url
SELECT image_embedding('clip/ViT-B-32-visual', 'https://link-to-your-image');
-- generate image embedding with image path (this path should be accessible from postgres server)
SELECT image_embedding('clip/ViT-B-32-visual', '/path/to/image/in-postgres-server');
-- get available list of models
SELECT get_available_models();
```

## Getting started

### Installing from precompiled binaries

You can download precompiled binaries for Mac and linux from Github releases page.
Make sure postgres is installed in your environment and `pg_config` is accessible form `$PATH`. Unzip the release archive from `lantern_extras` the directory run:

```bash
make install
```

### Building from source

<details>
<summary> Click to expand</summary>

You should have onnxruntime in your system in order to run the extension.
You can download the `onnxruntime` binary realease from GitHub https://github.com/microsoft/onnxruntime/releases/tag/v1.16.1 and place it somewhere in your system (e.g. /usr/lib/onnxruntime)

Then you should export these 2 environment variables

```bash
export ORT_STRATEGY=system
export ORT_DYLIB_PATH=/usr/local/lib/onnxruntime/lib/libonnxruntime.so
```

In some systems you will need to specify `dlopen` search path, so the extension could load `ort` inside postgres.

To do that create a file `/etc/ld.so.conf.d/onnx.conf` with content `/usr/local/lib/onnxruntime/lib` and run `ldconfig`

This extension is written in Rust so requires Rust toolchain. Make sure Rust toolchain is installed before continuing
The extension also uses `pgrx`. If pgrx is not already installed, use the following commands to install it:

```
#install pgrx prerequisites
sudo apt install pkg-config libssl-dev zlib1g-dev libreadline-dev
sudo apt-get install clang

#install pgrx itself
cargo install --locked cargo-pgrx --version 0.11.3
cargo pgrx init --pg15 $(which pg_config)
```

Then, you can run the extension under development with the following

```bash
cargo pgrx run --package lantern_extras # runs in a testing environment
```

To package the extension run

```bash
cargo pgrx package --package lantern_extras
```

To install the extension run

```bash
cargo pgrx install --release --pg-config /usr/bin/pg_config --package lantern_extras
```
 
</details>

### Initializing with psql

Once the extension is installed, in a psql shell or in your favorite SQL environment run:

```sql
CREATE EXTENSION lantern_extras;
```

### Adding new models

To add new textual or visual models for generating vector embeddings you can follow this steps:

1. Find the model onnx file or convert it using [optimum-cli](https://huggingface.co/docs/transformers/serialization). Example `optimum-cli export onnx --model BAAI/bge-base-en onnx/`
2. Host the onnx model
3. Add model information in `MODEL_INFO_MAP` under `lantern_extras/src/encoder.rs`
4. Add new image/text processor based on model inputs (you can check existing processors they might match the model) and then add the `match` arm in `process_text` or `process_image` function in `EncoderService` so it will run corresponding processor for model.

After this your model should be callable from SQL like

```sql
SELECT text_embedding('your/model_name', 'Your text');
```

## Lantern Daemon in SQL
To enable the daemon add `lantern_extra.so` to `shared_preload_libraries` in `postgresql.conf` file and set the `lantern_extras.enable_daemon` GUC to true. This can be done by executing the following command:

```sql
ALTER SYSTEM SET lantern_extras.enable_daemon = true;
SELECT pg_reload_conf();
```
The daemon will start, targeting the current connected database or databases specified in the `lantern_extras.daemon_databases` GUC.

**Important Notes**  
This is an experimental functionality to enable lantern daemon from SQL

### SQL Functions for Embedding Jobs
This functions can be used both with externally managed Lantern Daemon or with a daemon run from the SQL.

**Adding an Embedding Job**  
To add a new embedding job, use the `add_embedding_job` function:

```sql
SELECT add_embedding_job(
    'table_name',        -- Name of the table
    'src_column',        -- Source column for embeddings
    'dst_column',        -- Destination column for embeddings
    'embedding_model',   -- Embedding model to use
    'runtime',           -- Runtime environment (default: 'ort')
    'runtime_params',    -- Runtime parameters (default: '{}')
    'pk',                -- Primary key column (default: 'id')
    'schema'             -- Schema name (default: 'public')
);
```

**Getting Embedding Job Status**  
To get the status of an embedding job, use the `get_embedding_job_status` function:

```sql
SELECT * FROM get_embedding_job_status(job_id);
```
This will return a table with the following columns:

- `status`: The current status of the job.
- `progress`: The progress of the job as a percentage.
- `error`: Any error message if the job failed.

**Getting All Embedding Jobs**  
To get the status of all embedding jobs, use the `get_embedding_jobs` function:

```sql
SELECT * FROM get_embedding_jobs();

```
This will return a table with the following columns:

- `id`: Id of the job
- `status`: The current status of the job.
- `progress`: The progress of the job as a percentage.
- `error`: Any error message if the job failed.

**Adding a Completion Job**  
To add a new completion job, use the `add_completion_job` function:

```sql
SELECT add_completion_job(
    'table_name',        -- Name of the table
    'src_column',        -- Source column for embeddings
    'dst_column',        -- Destination column for embeddings
    'context',           -- System prompt to be used for LLM (default: lantern_extras.completion_context GUC)
    'column_type',       -- Target column type to be used for destination (default: TEXT)
    'model',             -- LLM model to use (default: 'gpt4-o')
    'runtime',           -- Runtime environment (default: 'openai')
    'runtime_params',    -- Runtime parameters (default: '{}' inferred from GUC variables)
    'pk',                -- Primary key column (default: 'id')
    'schema'             -- Schema name (default: 'public')
);
```

**Getting All Completion Jobs**  
To get the status of all completion jobs, use the `get_completion_jobs` function:

```sql
SELECT * FROM get_completion_jobs();

```
This will return a table with the following columns:

- `id`: Id of the job
- `status`: The current status of the job.
- `progress`: The progress of the job as a percentage.
- `error`: Any error message if the job failed.

**Canceling an Embedding Job**  
To cancel an embedding job, use the `cancel_embedding_job` function:

```sql
SELECT cancel_embedding_job(job_id);
```

**Resuming an Embedding Job**  
To resume a paused embedding job, use the `resume_embedding_job` function:

```sql
SELECT resume_embedding_job(job_id);
```

**Getting All Failed Rows for Completion Job**  
To get failed rows for completion job, use the `get_completion_job_failures(job_id)` function:

```sql
SELECT * FROM get_completion_job_failures(1);

```
This will return a table with the following columns:

- `row_id`: Primary key of the failed row in source table
- `value`: The value returned from LLM response

### LLM Query

***Calling Chat OpenAI Completion API***
```sql
SET lantern_extras.openai_token='xxxx';
SELECT openai_completion(query, [model, context, base_url]);
```

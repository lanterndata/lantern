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

You should have onnxruntime in your system in order to build the extension.  
You can download the `onnxruntime` binary realease from GitHub https://github.com/microsoft/onnxruntime/releases/tag/v1.15.1 and place it somewhere in your system (e.g. /usr/lib/onnxruntime)

Then you should export these 2 environment variables

```bash
export ORT_STRATEGY=system
export ORT_LIB_LOCATION=/usr/local/lib/onnxruntime
```

And also add this configuration under `.cargo/config`

```
[target.aarch64-unknown-linux-gnu]
rustflags = ["-C", "link-args=-Wl,-rpath,/usr/local/lib/onnxruntime/lib"]
[target.'cfg(target_os="macos")']
# Postgres symbols won't be available until runtime
rustflags = ["-Clink-arg=-Wl,-undefined,dynamic_lookup"]
```

_replace `aarch64-unknown-linux-gnu` with your architecture. You can get it by running `rustc -vV | sed -n 's|host: ||p'`_

This extension is written in Rust so requires Rust toolchain. It uses `pgrx`

```bash
cargo pgrx run --package lantern_extras # runs in a testing environment
```

To package the extension run

```bash
cargo pgrx package --package lantern_extras
```

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

## Lantern Index Builder

## Description

This is a CLI application that creates an index for Lantern outside of Postgres which can later be imported into Postgres. This allows for faster index creation through parallelization.

## How to use

### Installation

Run `cargo install --path lantern_create_index` to install the binary

### Usage

Run `ldb-create-index --help` to show the cli options.

```bash
Usage: ldb-create-index --uri <URI> --table <TABLE> --column <COLUMN> -m <M> --efc <EFC> --ef <EF> -d <DIMS> --metric-kind <METRIC_KIND> --out <OUT>
```

### Example

```bash
ldb-create-index -u "postgresql://localhost/test" -t "small_world" -c "vec" -m 16 --ef 64 --efc 128 -d 3 --metric-kind cos --out /tmp/index.usearch
```

### Notes

The index should be created from the same database on which it will be loaded, so row tids will match later.

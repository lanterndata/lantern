# LanternDB Extras

This extension makes it a brease to experiment with embeddings from inside a postgres database. We use this extension along with [LanternDB](https://github.com/lanterndata/lanterndb) to make vector operations performant. But all the helpers here are standalone and may be used without the main database.

__NOTE__: Functions defined in this extension use postgres in ways postgres is usually not used. 
Some calls may result in large file downloads, or cpu-intensive model inference operations. Keep this in mind when using this extension a shared postgres environment.

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

## Getting started

### Installing from precompiled binaries

You can download precompiled binaries for Mac and linux from Github releases page.
Make sure postgres is installed in your environment and `pg_config` is accessible form `$PATH`. Unzip the release archive from `lanterndb_extras` the directory run:

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
rustflags = ["-C", "link-args=-Wl,-rpath,/usr/local/lib/onnx/lib"]
```
*replace `aarch64-unknown-linux-gnu` with your architecture. You can get it by running `rustc -vV | sed -n 's|host: ||p'`*  

This extension is written in Rust so requires Rust toolchain. It uses `pgrx`

```bash
cargo build
cargo package
cargo pgrx run --package lanterndb_extras # runs in a testing environment
```

### Initializing with psql

Once the extension is installed, in a psql shell or in your favorite SQL environment run:
```sql
CREATE EXTENSION lanterndb_extras;
```

## LanternDB Index Builder

## Description
This is a cli applications, which will help to create an index file for LanternDB, which will can later be imported into database.
Advantages of this project againts casual index creation is the parallelization of the job.

## How to use

### Installation

Run `cargo install --path lanterndb_create_index` to install the binary

### Usage

Run `ldb-create-index --help` to show the cli options.

```
Usage: ldb-create-index --uri <URI> --table <TABLE> --column <COLUMN> -m <M> --efc <EFC> --ef <EF> -d <DIMS> --metric-kind <METRIC_KIND> --out <OUT>
```

### Example

```
ldb-create-index -u "postgresql://localhost/test" -t "small_world" -c "vec" -m 16 --ef 64 --efc 128 -d 3 --metric-kind cos --out /tmp/index.usearch
```

### Notes
The index should be created from the same database on which it will be loaded, so row tids will match later.  
Currently version of usearch is not up to date, so it will match with LanternDB's usearch version, but this version has bugs when creating index with more than 8k items.


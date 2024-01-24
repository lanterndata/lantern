Thanks for considering contributing! The information below is intended to help you contribute.

## Running tests

```bash
# run all regression tests
make test

# only run regression tests that have $FILTER in regression sql file path
make test FILTER=hnsw

# run parallel tests
make test-parallel
```

Running `make test` will run the lantern regression tests, these run independent of one another. At the moment the tests for `make test-parallel` are under development, they can be found in `test/parallel`. The goal of the parallel tests is to generate a more realistic workload on the index to discover timing errors and other bugs dependent on more complex use, they run in the same database.

## Running benchmarks

This requires Python to be installed. Please check the `Dockerfile.dev` for pip requirements.

```bash
# set up benchmarking, run benchmarks, and print results
make benchmark

# run benchmarks and print results (skip setup)
make benchmark-skip-setup

# print most recent benchmark results (skip setup and running benchmarks)
make benchmark-print-only
```

## VSCode and IntelliSense

`.vscode/c_cpp_properties` is configured to use `./build/compile_commands.json`.
If you build Lantern in a different directory, make sure to update `.vscode` config appropriately in order to have IntelliSense working.

## Debugging the C codebase

If you make changes to the C codebase, in addition to `make test` and `make parallel-test`, you can also use the `livedebug.py` utility in a `tmux` session to easily attach `gdb` to the psql backend and find out what breaks.
Below is a short recording demonstrating the use of `livedebug.py`:

[![asciicast](https://asciinema.org/a/jTsbWdOcTvUl4iAJlAw3Cszbt.svg)](https://asciinema.org/a/jTsbWdOcTvUl4iAJlAw3Cszbt)

## Running sanitizers

To ensure that code is safe, pull requests are tested using google's [AddressSanitizer](https://github.com/google/sanitizers/wiki/AddressSanitizer). Additionally [UBSan](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html) is run against releases. A [docker container](scripts/sanitizers/Dockerfile) is provided for testing changes locally. it can be invoked by running the script `scripts/sanitizers/run_sanitizers.sh`. **Please note that this script must be run in the root directory of the lantern repository**. By default it will build `postgres 15.4` and run tests against it instrumented only with AddressSanitizer. If you would like to run UBSan you can pass the `-u` flag. If you wish to test against a specific version you can use the `-v` flag specifying a specific version, e.g. `scripts/sanitizers/run_sanitizers.sh -u -v11.21`

## Getting code coverage report locally

Make sure `lcov` is installed. It is installed in our development dockerfile (Dockerfile.dev)

Then, run the following (or equivalent) from the root of the repository, in order to:

1. Configure a coverage build via cmake
2. Build and install a coverage-enabled binary
3. Run the tests (you can run other workloads here as well)
4. Generate a coverage report

```
mkdir -p build_coverage && (cd build_coverage && cmake .. -DCODECOVERAGE=1 && sudo make install -j && make test && make cover)
```

## Adding/modifying LanternDB's SQL interface

When modifying the SQL interface, you add relevant SQL logic under `sql/`. In addition, you add an update script under `sql/updates`, in a file named `[CURRENT_VERSION]--latest.sql`. You should create this file if it does not exist.

Note that you never modify an already existing update file that does not have `latest` in its name.

The files that do not have `latest` in the name are part of a previous releases and help LanternDB users update to a newer version of the extension via `ALTER EXTENSION lantern UPDATE`.

## Browsing the Postgres repository offline

You can download PostgreSQL source code from [their ftp server](https://www.postgresql.org/ftp/source/). Alternatively, can clone their git repository.

```bash
# full repository
git clone https://git.postgresql.org/git/postgresql.git
# release head only
git clone --single-branch --branch REL_15_STABLE https://git.postgresql.org/git/postgresql.git --depth=1
```

## Preparing a release

1. Update LANTERN_VERSION variable at the top of CMakeLists.txt file
2. Prepare the SQL update script for the release
   1. If there already is an update script for the current release with a 'latest' suffix, rename it according to the version name being released
   2. If there is no such file, create an empty update file for the current release
3. Build the project with `cmake -DBUILD_FOR_DISTRIBUTING=YES` that will embed cmake version number into the binary.
   Alternatively, if you want to embed a different version name into the binary, build with -DRELEASE_ID=\[version name\] where the version name is the name of the release and the name used in update file above

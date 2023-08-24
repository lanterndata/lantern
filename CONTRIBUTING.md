Thanks for considering to contribute!
The information below is intended to *help* you contribute.

## Runing tests

```
-- run all regression tests
make test

-- only run regression tests that have $FILTER in regression sql file path
make test FILTER=hnsw
```

## VSCode and IntelliSense

`.vscode/c_cpp_properties` is configured to use `./build/compile_commands.json`.
If you build lanterndb in a different directory, make sure to update ``.vscode` config appropriately
in order to have IntelliSense working.

## Debugging the C codebase

If you make changes to the C codebase, in addition to `make test`, you can also use the `livedebug.py` utility 
in a `tmux` session to easily attach `gdb` to the psql backend and find out what breaks.
Below is a short recording demonstrating the use of `livedebug.py`:

[![asciicast](https://asciinema.org/a/jTsbWdOcTvUl4iAJlAw3Cszbt.svg)](https://asciinema.org/a/jTsbWdOcTvUl4iAJlAw3Cszbt)

## Adding/modifying LanternDB's SQL interface

When modifying the SQL interface, you add relevant SQL logic under `sql/`. In addition, you add an update script
under `sql/updates`, in a file named `[CURRENT_VERSION]--latest.sql`. You should create this file if it does not exist.

Note that you never modify an already existing update file that does not have `latest` in its name.
The files that do not have `latest` in the name are part of a previous releases and help LanternDB users update
to a newer version of the extension via `ALTER EXTENSION lanterndb UPDATE`.

## Browsing the Postgres repository offline

You can download PostgreSQL source code from [their ftp server](https://www.postgresql.org/ftp/source/). Alternatively, can clone their git repository.

```bash
#full repository
git clone https://git.postgresql.org/git/postgresql.git
#release head only
git clone --single-branch --branch REL_15_STABLE https://git.postgresql.org/git/postgresql.git --depth=1

```

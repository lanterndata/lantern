# Prerequisites.md
This doc contains the prerequisites that need to be fullfilled in order to run `make test` outlined in the [CONTRIBUTING](./CONTRIBUTING.md) doc.

## Fedora & Redhat pre-requisites
1. Install `cmake` and `gcc` to make sure you have basic build tools
    ```bash
    $ sudo dnf intsall cmake gcc-c++
    ```
1.  Install postgres extension development headers. Needed to get files like `postgres.h`.
    ```bash
    $ sudo dnf install postgresql-server-devel
    ```
    You should see a "postgres.h" file when you run the following:
    ```bash
    $  ls $(pg_config --includedir-server)
    ```
1. Install postgres (https://www.postgresql.org/download/linux/redhat/)
    ```bash
    $ sudo dnf install postgresql-server postgresql-contrib
    ```
    Running the following should now show that postgres server is disabled
    ```bash
    $ systemctl status postgresql
    ```
    Configure and start postgres:
    ```bash
    $ sudo postgresql-setup --initdb
    $ sudo systemctl start postgresql.service
    ```
    Now, you should see a bunch of processes (postmaster, walwriter, autovacuum, etc).
    ```bash
    $ systemctl status postgresql.service
    ```
1. Configure postgres so you can use psql from your default username.
    First, figure out your username
    ```bash
    $ whoami
    ```
    Then, create a postgres SUPERUSER role with your username.
    If you don't do this, you won't be able to use
    psql CLI from your default username to communicate
    with postgres, and this will cause `make tests` to fail.
    ```bash
    $ sudo -u postgres psql
    postgres> CREATE ROLE ${YOUR_USERNAME_HERE} SUPERUSER LOGIN;
    ```
    Then, make sure you see your username below.
    ```bash
    postgres> \du
    postgres> exit
    ```
    Finally, a database with your username has to exist for some reason.
    ```
    $ createdb $(whoami)
    ```
    Now, if you type just `psql`, you should see a psql shell.
1. Install pgvector.
    Best option is to install from source: https://github.com/pgvector/pgvector/blob/master/README.md.
    It's quick and easy.

    Alternatively, you can install the postgres RPM https://www.postgresql.org/download/linux/redhat/and
    then run `sudo dnf pgvector_15`. However, doing this led me to have the following error:
    ```bash
    $ psql
    YOUR_USERNAME> CREATE EXTENSION vector;
    ERROR:  extension "vector" is not available
    DETAIL:  Could not open extension control file "/usr/share/pgsql/extension/vector.control": No such file or directory.
    ```
    This is because the extension was installed to an incorrect path:
    ```bash
    $ sudo find / -name "vector.control" -print
    /usr/pgsql-15/share/extension/vector.control
    ```
    Not sure why the extension was installed in the wrong path, but installing from source
    is a better option. Make sure to build from a release tag for stability.
1. If you go back to [README](./README.md) and try to follow build instructions, your build will still fail.
    Run the following before doing so:
    ```bash
    $ sudo dnf install redhat-rpm-config
    $ export RPM_PACKAGE_NAME="0.1.3"
    $ export RPM_PACKAGE_VERSION="0.1.3"
    $ export RPM_PACKAGE_RELEASE="noop"
    $ export RPM_ARCH=$(uname -m)
    ```
    Not sure who is depending on this package, but this needs to be figured out
    as the steps above are hacky.

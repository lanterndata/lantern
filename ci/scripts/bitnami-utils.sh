#!/usr/bin/env bash
# Helper functions copied from bitnami/postgres image

########################
# Run command as a specific user and group (optional)
# Arguments:
#   $1 - USER(:GROUP) to switch to
#   $2..$n - command to execute
# Returns:
#   Exit code of the specified command
#########################
run_as_user() {
    run_chroot "$@"
}

########################
# Execute command as a specific user and group (optional),
# replacing the current process image
# Arguments:
#   $1 - USER(:GROUP) to switch to
#   $2..$n - command to execute
# Returns:
#   Exit code of the specified command
#########################
exec_as_user() {
    run_chroot --replace-process "$@"
}

########################
# Run a command using chroot
# Arguments:
#   $1 - USER(:GROUP) to switch to
#   $2..$n - command to execute
# Flags:
#   -r | --replace-process - Replace the current process image (optional)
# Returns:
#   Exit code of the specified command
#########################
run_chroot() {
    local userspec
    local user
    local homedir
    local replace=false
    local -r cwd="$(pwd)"

    # Parse and validate flags
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            -r | --replace-process)
                replace=true
                ;;
            --)
                shift
                break
                ;;
            -*)
                stderr_print "unrecognized flag $1"
                return 1
                ;;
            *)
                break
                ;;
        esac
        shift
    done

    # Parse and validate arguments
    if [[ "$#" -lt 2 ]]; then
        echo "expected at least 2 arguments"
        return 1
    else
        userspec=$1
        shift

        # userspec can optionally include the group, so we parse the user
        user=$(echo "$userspec" | cut -d':' -f1)
    fi

    if ! am_i_root; then
        error "Could not switch to '${userspec}': Operation not permitted"
        return 1
    fi

    # Get the HOME directory for the user to switch, as chroot does
    # not properly update this env and some scripts rely on it
    homedir=$(eval echo "~${user}")
    if [[ ! -d $homedir ]]; then
        homedir="${HOME:-/}"
    fi

    # Obtaining value for "$@" indirectly in order to properly support shell parameter expansion
    if [[ "$replace" = true ]]; then
        exec chroot --userspec="$userspec" / bash -c "cd ${cwd}; export HOME=${homedir}; exec \"\$@\"" -- "$@"
    else
        chroot --userspec="$userspec" / bash -c "cd ${cwd}; export HOME=${homedir}; exec \"\$@\"" -- "$@"
    fi
}
########################
# Check if the script is currently running as root
# Arguments:
#   $1 - user
#   $2 - group
# Returns:
#   Boolean
#########################
am_i_root() {
    if [[ "$(id -u)" = "0" ]]; then
        true
    else
        false
    fi
}
########################
# Ensure a directory exists and, optionally, is owned by the given user
# Arguments:
#   $1 - directory
#   $2 - owner
# Returns:
#   None
#########################
ensure_dir_exists() {
    local dir="${1:?directory is missing}"
    local owner_user="${2:-}"
    local owner_group="${3:-}"

    [ -d "${dir}" ] || mkdir -p "${dir}"
    if [[ -n $owner_user ]]; then
        owned_by "$dir" "$owner_user" "$owner_group"
    fi
}

########################
# Checks whether a directory is empty or not
# arguments:
#   $1 - directory
# returns:
#   boolean
#########################
is_dir_empty() {
    local -r path="${1:?missing directory}"
    # Calculate real path in order to avoid issues with symlinks
    local -r dir="$(realpath "$path")"
    if [[ ! -e "$dir" ]] || [[ -z "$(ls -A "$dir")" ]]; then
        true
    else
        false
    fi
}
########################
# Read the provided pid file and returns a PID
# Arguments:
#   $1 - Pid file
# Returns:
#   PID
#########################
get_pid_from_file() {
    local pid_file="${1:?pid file is missing}"

    if [[ -f "$pid_file" ]]; then
        if [[ -n "$(< "$pid_file")" ]] && [[ "$(< "$pid_file")" -gt 0 ]]; then
            echo "$(< "$pid_file")"
        fi
    fi
}
########################
# Check if a provided PID corresponds to a running service
# Arguments:
#   $1 - PID
# Returns:
#   Boolean
#########################
is_service_running() {
    local pid="${1:?pid is missing}"

    kill -0 "$pid" 2>/dev/null
}
########################
# Replace a regex-matching string in a file
# Arguments:
#   $1 - filename
#   $2 - match regex
#   $3 - substitute regex
#   $4 - use POSIX regex. Default: true
# Returns:
#   None
#########################
replace_in_file() {
    local filename="${1:?filename is required}"
    local match_regex="${2:?match regex is required}"
    local substitute_regex="${3:?substitute regex is required}"
    local posix_regex=${4:-true}

    local result

    # We should avoid using 'sed in-place' substitutions
    # 1) They are not compatible with files mounted from ConfigMap(s)
    # 2) We found incompatibility issues with Debian10 and "in-place" substitutions
    local -r del=$'\001' # Use a non-printable character as a 'sed' delimiter to avoid issues
    if [[ $posix_regex = true ]]; then
        result="$(sed -E "s${del}${match_regex}${del}${substitute_regex}${del}g" "$filename")"
    else
        result="$(sed "s${del}${match_regex}${del}${substitute_regex}${del}g" "$filename")"
    fi
    echo "$result" > "$filename"
}
########################
# Change a PostgreSQL configuration file by setting a property
# Globals:
#   POSTGRESQL_*
# Arguments:
#   $1 - property
#   $2 - value
#   $3 - Path to configuration file (default: $POSTGRESQL_CONF_FILE)
# Returns:
#   None
#########################
postgresql_set_property() {
    local -r property="${1:?missing property}"
    local -r value="${2:?missing value}"
    local -r conf_file="${3:-$POSTGRESQL_CONF_FILE}"
    local psql_conf
    if grep -qE "^#*\s*${property}" "$conf_file" >/dev/null; then
        replace_in_file "$conf_file" "^#*\s*${property}\s*=.*" "${property} = '${value}'" false
    else
        echo "${property} = '${value}'" >>"$conf_file"
    fi
}

########################
# Create a user for master-slave replication
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_create_replication_user() {
    local -r escaped_password="${POSTGRESQL_REPLICATION_PASSWORD//\'/\'\'}"
    echo "Creating replication user $POSTGRESQL_REPLICATION_USER"
    echo "CREATE ROLE \"$POSTGRESQL_REPLICATION_USER\" REPLICATION LOGIN ENCRYPTED PASSWORD '$escaped_password'" | postgresql_execute
}
########################
# Return PostgreSQL major version
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   String
#########################
postgresql_get_major_version() {
    psql --version | grep -oE "[0-9]+\.[0-9]+" | grep -oE "^[0-9]+" | head -n 1  | tr -d '\n'
}
########################
# Execute an arbitrary query/queries against the running PostgreSQL service and print the output
# Stdin:
#   Query/queries to execute
# Globals:
#   BITNAMI_DEBUG
#   POSTGRESQL_*
# Arguments:
#   $1 - Database where to run the queries
#   $2 - User to run queries
#   $3 - Password
#   $4 - Extra options (eg. -tA)
# Returns:
#   None
#########################
postgresql_execute() {
    local -r db="${1:-}"
    local -r user="${2:-postgres}"
    local -r pass="${3:-}"
    local opts
    read -r -a opts <<<"${@:4}"

    local args=("-U" "$user" "-p" "${POSTGRESQL_PORT_NUMBER:-5432}")
    [[ -n "$db" ]] && args+=("-d" "$db")
    [[ "${#opts[@]}" -gt 0 ]] && args+=("${opts[@]}")

    # Execute the Query/queries from stdin
    PGPASSWORD=$pass psql "${args[@]}"
}
########################
# Change pg_hba.conf so it allows access from replication users
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_add_replication_to_pghba() {
    local replication_auth="trust"
    if [[ -n "$POSTGRESQL_REPLICATION_PASSWORD" ]]; then
        replication_auth="md5"
    fi
    cat <<EOF >>"$POSTGRESQL_PGHBA_FILE"
host      replication     all             0.0.0.0/0               ${replication_auth}
host      replication     all             ::/0                    ${replication_auth}
EOF
}
########################
# Change pg_hba.conf so it allows local UNIX socket-based connections
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_allow_local_connection() {
    cat <<EOF >>"$POSTGRESQL_PGHBA_FILE"
local    all             all                                     trust
host     all             all        127.0.0.1/32                 trust
host     all             all        ::1/128                      trust
EOF
}

########################
# Change postgresql.conf by setting replication parameters
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_configure_replication_parameters() {
    local -r psql_major_version="$(postgresql_get_major_version)"
    echo "Configuring replication parameters"
    postgresql_set_property "wal_level" "$POSTGRESQL_WAL_LEVEL"
    postgresql_set_property "max_wal_size" "400MB"
    postgresql_set_property "max_wal_senders" "16"
    if [[ "$psql_major_version" == "11" || "$psql_major_version" == "12" ]]; then
        postgresql_set_property "wal_keep_segments" "12"
    else
        postgresql_set_property "wal_keep_size" "128MB"
    fi
    postgresql_set_property "hot_standby" "on"
}

########################
# Change postgresql.conf by setting parameters for synchronous replication
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_configure_synchronous_replication() {
    local replication_nodes=""
    local synchronous_standby_names=""
    echo "Configuring synchronous_replication"

    # Check for comma separate values
    # When using repmgr, POSTGRESQL_CLUSTER_APP_NAME will contain the list of nodes to be synchronous
    # This list need to cleaned from other things but node names.
    if [[ "$POSTGRESQL_CLUSTER_APP_NAME" == *","* ]]; then
        read -r -a nodes <<<"$(tr ',;' ' ' <<<"${POSTGRESQL_CLUSTER_APP_NAME}")"
        for node in "${nodes[@]}"; do
            [[ "$node" =~ ^(([^:/?#]+):)?// ]] || node="tcp://${node}"

            # repmgr is only using the first segment of the FQDN as the application name
            host="$(parse_uri "$node" 'host' | awk -F. '{print $1}')"
            replication_nodes="${replication_nodes}${replication_nodes:+,}\"${host}\""
        done
    else
        replication_nodes="\"${POSTGRESQL_CLUSTER_APP_NAME}\""
    fi

    if ((POSTGRESQL_NUM_SYNCHRONOUS_REPLICAS > 0)); then
        synchronous_standby_names="${POSTGRESQL_NUM_SYNCHRONOUS_REPLICAS} (${replication_nodes})"
        if [[ -n "$POSTGRESQL_SYNCHRONOUS_REPLICAS_MODE" ]]; then
            synchronous_standby_names="${POSTGRESQL_SYNCHRONOUS_REPLICAS_MODE} ${synchronous_standby_names}"
        fi

        postgresql_set_property "synchronous_commit" "$POSTGRESQL_SYNCHRONOUS_COMMIT_MODE"
        postgresql_set_property "synchronous_standby_names" "$synchronous_standby_names"
    fi
}
########################
# Change postgresql.conf by setting TLS properies
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_configure_tls() {
    echo "Configuring TLS"
    chmod 600 "$POSTGRESQL_TLS_KEY_FILE" || warn "Could not set compulsory permissions (600) on file ${POSTGRESQL_TLS_KEY_FILE}"
    postgresql_set_property "ssl" "on"
    # Server ciphers are preferred by default
    ! is_boolean_yes "$POSTGRESQL_TLS_PREFER_SERVER_CIPHERS" && postgresql_set_property "ssl_prefer_server_ciphers" "off"
    [[ -n $POSTGRESQL_TLS_CA_FILE ]] && postgresql_set_property "ssl_ca_file" "$POSTGRESQL_TLS_CA_FILE"
    [[ -n $POSTGRESQL_TLS_CRL_FILE ]] && postgresql_set_property "ssl_crl_file" "$POSTGRESQL_TLS_CRL_FILE"
    postgresql_set_property "ssl_cert_file" "$POSTGRESQL_TLS_CERT_FILE"
    postgresql_set_property "ssl_key_file" "$POSTGRESQL_TLS_KEY_FILE"
}

########################
# Change postgresql.conf by setting fsync
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_configure_fsync() {
    echo "Configuring fsync"
    postgresql_set_property "fsync" "$POSTGRESQL_FSYNC"
}

########################
# Alter password of the postgres user
# Globals:
#   POSTGRESQL_*
# Arguments:
#   Password
# Returns:
#   None
#########################
postgresql_alter_postgres_user() {
    local -r escaped_password="${1//\'/\'\'}"
    echo "Changing password of postgres"
    echo "ALTER ROLE postgres WITH PASSWORD '$escaped_password';" | postgresql_execute
    if [[ -n "$POSTGRESQL_POSTGRES_CONNECTION_LIMIT" ]]; then
        echo "ALTER ROLE postgres WITH CONNECTION LIMIT ${POSTGRESQL_POSTGRES_CONNECTION_LIMIT};" | postgresql_execute
    fi
}

########################
# Create an admin user with all privileges in POSTGRESQL_DATABASE
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_create_admin_user() {
    local -r escaped_password="${POSTGRESQL_PASSWORD//\'/\'\'}"
    echo "Creating user ${POSTGRESQL_USERNAME}"
    local connlimit_string=""
    if [[ -n "$POSTGRESQL_USERNAME_CONNECTION_LIMIT" ]]; then
        connlimit_string="CONNECTION LIMIT ${POSTGRESQL_USERNAME_CONNECTION_LIMIT}"
    fi
    echo "CREATE ROLE \"${POSTGRESQL_USERNAME}\" WITH LOGIN ${connlimit_string} CREATEDB PASSWORD '${escaped_password}';" | postgresql_execute
    echo "Granting access to \"${POSTGRESQL_USERNAME}\" to the database \"${POSTGRESQL_DATABASE}\""
    echo "GRANT ALL PRIVILEGES ON DATABASE \"${POSTGRESQL_DATABASE}\" TO \"${POSTGRESQL_USERNAME}\"\;" | postgresql_execute "" "postgres" "$POSTGRESQL_PASSWORD"
    echo "ALTER DATABASE \"${POSTGRESQL_DATABASE}\" OWNER TO \"${POSTGRESQL_USERNAME}\"\;" | postgresql_execute "" "postgres" "$POSTGRESQL_PASSWORD"
    echo "Setting ownership for the 'public' schema database \"${POSTGRESQL_DATABASE}\" to \"${POSTGRESQL_USERNAME}\""
    echo "ALTER SCHEMA public OWNER TO \"${POSTGRESQL_USERNAME}\"\;" | postgresql_execute "$POSTGRESQL_DATABASE" "postgres" "$POSTGRESQL_PASSWORD"
}

########################
# Create a database with name $POSTGRESQL_DATABASE
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_create_custom_database() {
    echo "CREATE DATABASE \"$POSTGRESQL_DATABASE\"" | postgresql_execute "" "postgres" ""
}

########################
# Change postgresql.conf to listen in 0.0.0.0
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_enable_remote_connections() {
    postgresql_set_property "listen_addresses" "*"
}

########################
# Check if a given configuration file was mounted externally
# Globals:
#   POSTGRESQL_*
# Arguments:
#   $1 - Filename
# Returns:
#   1 if the file was mounted externally, 0 otherwise
#########################
postgresql_is_file_external() {
    local -r filename=$1
    if [[ -d "$POSTGRESQL_MOUNTED_CONF_DIR" ]] && [[ -f "$POSTGRESQL_MOUNTED_CONF_DIR"/"$filename" ]]; then
        return 0
    else
        return 1
    fi
}

########################
# Remove flags and postmaster files from a previous run (case of container restart)
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_clean_from_restart() {
    local -r -a files=(
        "$POSTGRESQL_DATA_DIR"/postmaster.pid
        "$POSTGRESQL_DATA_DIR"/standby.signal
        "$POSTGRESQL_DATA_DIR"/recovery.signal
    )

    for file in "${files[@]}"; do
        if [[ -f "$file" ]]; then
            echo "Cleaning stale $file file"
            rm "$file"
        fi
    done
}

########################
# Create local auth configuration in pg_hba
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_create_pghba() {
    echo "Generating local authentication configuration"
    cat <<EOF >"$POSTGRESQL_PGHBA_FILE"
host     all             all             0.0.0.0/0               trust
host     all             all             ::/0                    trust
EOF
}
########################
# Ensure PostgreSQL is initialized
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_initialize() {
    echo "Initializing PostgreSQL database..."
    POSTGRESQL_CLUSTER_APP_NAME='walreceiver'
    POSTGRESQL_PGHBA_FILE="$POSTGRESQL_CONF_DIR/pg_hba.conf"
    POSTGRESQL_CONF_FILE="$POSTGRESQL_CONF_DIR/postgresql.conf"
    POSTGRESQL_LOG_FILE="$POSTGRESQL_CONF_DIR/pg.log"
    POSTGRESQL_SHUTDOWN_MODE='fast'
    POSTGRESQL_PGCTLTIMEOUT=120
    POSTGRESQL_WAL_LEVEL='replica'
    POSTGRESQL_FSYNC='on'
    POSTGRESQL_PASSWORD='postgres' 
    POSTGRESQL_REPLICATION_USER='repl' 
    POSTGRESQL_DAEMON_USER=postgres
    POSTGRESQL_DAEMON_GROUP=postgres
    POSTGRESQL_INIT_MAX_TIMEOUT=120
    POSTGRESQL_BIN_DIR=$(pg_config --bindir)
    mkdir $POSTGRESQL_DATA_DIR
    mkdir $POSTGRESQL_CONF_DIR
    cp -f "$(pg_config --sharedir)/postgresql.conf.sample" "$POSTGRESQL_CONF_FILE"
    cp -f "$(pg_config --sharedir)/pg_hba.conf.sample" "$POSTGRESQL_PGHBA_FILE"

    chmod u+rwx "$POSTGRESQL_DATA_DIR" || echo "Lack of permissions on data directory!"
    chmod go-rwx "$POSTGRESQL_DATA_DIR" || echo "Lack of permissions on data directory!"
    postgresql_create_pghba && postgresql_allow_local_connection
    # Configure port
    postgresql_set_property "port" "$POSTGRESQL_PORT_NUMBER"
    if [[ "$POSTGRESQL_REPLICATION_MODE" = "master" ]]; then
        postgresql_master_init_db
        postgresql_start_bg "false"
        postgresql_alter_postgres_user "$POSTGRESQL_PASSWORD"
        [[ -n "$POSTGRESQL_REPLICATION_USER" ]] && postgresql_create_replication_user
        postgresql_configure_replication_parameters
        postgresql_configure_synchronous_replication
        postgresql_configure_fsync
        [[ -n "$POSTGRESQL_REPLICATION_USER" ]] && postgresql_add_replication_to_pghba
    else
        postgresql_slave_init_db
        postgresql_configure_replication_parameters
        postgresql_configure_fsync
        postgresql_configure_recovery
    fi

    # Delete conf files generated on first run
    rm -f "$POSTGRESQL_DATA_DIR"/postgresql.conf "$POSTGRESQL_DATA_DIR"/pg_hba.conf

    # Stop postgresql
    postgresql_stop
    postgresql_start_bg
}


########################
# Stop PostgreSQL
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   None
#########################
postgresql_stop() {
    local -r -a cmd=("pg_ctl" "stop" "-w" "-D" "$POSTGRESQL_DATA_DIR" "-m" "$POSTGRESQL_SHUTDOWN_MODE" "-t" "$POSTGRESQL_PGCTLTIMEOUT")
    if [[ -f "$POSTGRESQL_PID_FILE" ]]; then
        echo "Stopping PostgreSQL..."
        if am_i_root; then
            run_as_user "$POSTGRESQL_DAEMON_USER" "${cmd[@]}"
        else
            "${cmd[@]}"
        fi
    fi
}

########################
# Start PostgreSQL and wait until it is ready
# Globals:
#   POSTGRESQL_*
# Arguments:
#   $1 - Enable logs for PostgreSQL. Default: false
# Returns:
#   None
#########################
postgresql_start_bg() {
    local -r pg_logs=${1:-false}
    local -r pg_ctl_flags=("-w" "-D" "$POSTGRESQL_DATA_DIR" "-l" "$POSTGRESQL_LOG_FILE" "-o" "--config-file=$POSTGRESQL_CONF_FILE --external_pid_file=$POSTGRESQL_PID_FILE --hba_file=$POSTGRESQL_PGHBA_FILE")
    echo "Starting PostgreSQL in background..."
    if is_postgresql_running; then
        return 0
    fi
    local pg_ctl_cmd=()
    if am_i_root; then
        pg_ctl_cmd+=("run_as_user" "$POSTGRESQL_DAEMON_USER")
    fi
    pg_ctl_cmd+=("$POSTGRESQL_BIN_DIR"/pg_ctl)
    if [[ "${BITNAMI_DEBUG:-false}" = true ]] || [[ $pg_logs = true ]]; then
        "${pg_ctl_cmd[@]}" "start" "${pg_ctl_flags[@]}"
    else
        "${pg_ctl_cmd[@]}" "start" "${pg_ctl_flags[@]}" >/dev/null 2>&1
    fi
    local pg_isready_args=("-U" "postgres" "-p" "$POSTGRESQL_PORT_NUMBER")
    local counter=$POSTGRESQL_INIT_MAX_TIMEOUT
    while ! "$POSTGRESQL_BIN_DIR"/pg_isready "${pg_isready_args[@]}" >/dev/null 2>&1; do
        sleep 1
        counter=$((counter - 1))
        if ((counter <= 0)); then
            echo "PostgreSQL is not ready after $POSTGRESQL_INIT_MAX_TIMEOUT seconds"
            exit 1
        fi
    done
}

########################
# Check if PostgreSQL is running
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   Boolean
#########################
is_postgresql_running() {
    local pid
    pid="$(get_pid_from_file "$POSTGRESQL_PID_FILE")"

    if [[ -z "$pid" ]]; then
        false
    else
        is_service_running "$pid"
    fi
}

########################
# Check if PostgreSQL is not running
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   Boolean
#########################
is_postgresql_not_running() {
    ! is_postgresql_running
}

########################
# Initialize master node database by running initdb
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   Boolean
#########################
postgresql_master_init_db() {
    local envExtraFlags=()
    local initdb_args=()
    if [[ -n "${POSTGRESQL_INITDB_ARGS}" ]]; then
        read -r -a envExtraFlags <<<"$POSTGRESQL_INITDB_ARGS"
        initdb_args+=("${envExtraFlags[@]}")
    fi
    if [[ -n "$POSTGRESQL_INITDB_WAL_DIR" ]]; then
        ensure_dir_exists "$POSTGRESQL_INITDB_WAL_DIR"
        am_i_root && chown "$POSTGRESQL_DAEMON_USER:$POSTGRESQL_DAEMON_GROUP" "$POSTGRESQL_INITDB_WAL_DIR"
        initdb_args+=("--waldir" "$POSTGRESQL_INITDB_WAL_DIR")
    fi
    local initdb_cmd=()
    if am_i_root; then
        initdb_cmd+=("run_as_user" "$POSTGRESQL_DAEMON_USER")
    fi
    initdb_cmd+=("$POSTGRESQL_BIN_DIR/initdb")
    if [[ -n "${initdb_args[*]:-}" ]]; then
        echo "Initializing PostgreSQL with ${initdb_args[*]} extra initdb arguments"
        if [[ "${BITNAMI_DEBUG:-false}" = true ]]; then
            "${initdb_cmd[@]}" -E UTF8 -D "$POSTGRESQL_DATA_DIR" -U "postgres" "${initdb_args[@]}"
        else
            "${initdb_cmd[@]}" -E UTF8 -D "$POSTGRESQL_DATA_DIR" -U "postgres" "${initdb_args[@]}" >/dev/null 2>&1
        fi
    elif [[ "${BITNAMI_DEBUG:-false}" = true ]]; then
        "${initdb_cmd[@]}" -E UTF8 -D "$POSTGRESQL_DATA_DIR" -U "postgres"
    else
        "${initdb_cmd[@]}" -E UTF8 -D "$POSTGRESQL_DATA_DIR" -U "postgres" >/dev/null 2>&1
    fi
}

########################
# Initialize slave node by running pg_basebackup
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   Boolean
#########################
postgresql_slave_init_db() {
    echo "Waiting for replication master to accept connections (${POSTGRESQL_INIT_MAX_TIMEOUT} timeout)..."
    local -r check_args=("-U" "$POSTGRESQL_REPLICATION_USER" "-h" "$POSTGRESQL_MASTER_HOST" "-p" "$POSTGRESQL_MASTER_PORT_NUMBER" "-d" "postgres")
    local check_cmd=()
    if am_i_root; then
        check_cmd=("run_as_user" "$POSTGRESQL_DAEMON_USER")
    fi
    check_cmd+=("$POSTGRESQL_BIN_DIR"/pg_isready)
    local ready_counter=$POSTGRESQL_INIT_MAX_TIMEOUT

    while ! PGPASSWORD=$POSTGRESQL_REPLICATION_PASSWORD "${check_cmd[@]}" "${check_args[@]}"; do
        sleep 1
        ready_counter=$((ready_counter - 1))
        if ((ready_counter <= 0)); then
            echo "PostgreSQL master is not ready after $POSTGRESQL_INIT_MAX_TIMEOUT seconds"
            exit 1
        fi

    done
    echo "Replicating the initial database"
    local -r backup_args=("-D" "$POSTGRESQL_DATA_DIR" "-U" "$POSTGRESQL_REPLICATION_USER" "-h" "$POSTGRESQL_MASTER_HOST" "-p" "$POSTGRESQL_MASTER_PORT_NUMBER" "-X" "stream" "-w" "-v" "-P")
    local backup_cmd=()
    if am_i_root; then
        backup_cmd+=("run_as_user" "$POSTGRESQL_DAEMON_USER")
    fi
    backup_cmd+=("$POSTGRESQL_BIN_DIR"/pg_basebackup)
    local replication_counter=$POSTGRESQL_INIT_MAX_TIMEOUT
    while ! PGPASSWORD=$POSTGRESQL_REPLICATION_PASSWORD "${backup_cmd[@]}" "${backup_args[@]}"; do
        debug "Backup command failed. Sleeping and trying again"
        sleep 1
        replication_counter=$((replication_counter - 1))
        if ((replication_counter <= 0)); then
            echo "Slave replication failed after trying for $POSTGRESQL_INIT_MAX_TIMEOUT seconds"
            exit 1
        fi
    done
}

########################
# Create recovery.conf in slave node
# Globals:
#   POSTGRESQL_*
# Arguments:
#   None
# Returns:
#   Boolean
#########################
postgresql_configure_recovery() {
    echo "Setting up streaming replication slave..."
    POSTGRESQL_RECOVERY_FILE="$POSTGRESQL_DATA_DIR/recovery.conf"
    local -r escaped_password="${POSTGRESQL_REPLICATION_PASSWORD//\&/\\&}"
    local -r psql_major_version="$(postgresql_get_major_version)"
    if [[ "$psql_major_version" == "11" ]]; then
        cp -f "$(pg_config --sharedir)/recovery.conf.sample" "$POSTGRESQL_RECOVERY_FILE"
        chmod 600 "$POSTGRESQL_RECOVERY_FILE"
        am_i_root && chown "$POSTGRESQL_DAEMON_USER:$POSTGRESQL_DAEMON_GROUP" "$POSTGRESQL_RECOVERY_FILE"
        postgresql_set_property "standby_mode" "on" "$POSTGRESQL_RECOVERY_FILE"
        postgresql_set_property "primary_conninfo" "host=${POSTGRESQL_MASTER_HOST} port=${POSTGRESQL_MASTER_PORT_NUMBER} user=${POSTGRESQL_REPLICATION_USER} password=${escaped_password} application_name=${POSTGRESQL_CLUSTER_APP_NAME}" "$POSTGRESQL_RECOVERY_FILE"
        postgresql_set_property "trigger_file" "/tmp/postgresql.trigger.${POSTGRESQL_MASTER_PORT_NUMBER}" "$POSTGRESQL_RECOVERY_FILE"
    else
        postgresql_set_property "primary_conninfo" "host=${POSTGRESQL_MASTER_HOST} port=${POSTGRESQL_MASTER_PORT_NUMBER} user=${POSTGRESQL_REPLICATION_USER} password=${escaped_password} application_name=${POSTGRESQL_CLUSTER_APP_NAME}" "$POSTGRESQL_CONF_FILE"
        postgresql_set_property "promote_trigger_file" "/tmp/postgresql.trigger.${POSTGRESQL_MASTER_PORT_NUMBER}" "$POSTGRESQL_CONF_FILE"
        touch "$POSTGRESQL_DATA_DIR"/standby.signal
    fi
}


start_postgres_master() {
  POSTGRESQL_REPLICATION_MODE='master' 
  POSTGRESQL_PORT_NUMBER=5442
  POSTGRESQL_DATA_DIR=/tmp/postgres-master
  POSTGRESQL_CONF_DIR=/tmp/postgres-master-conf
  POSTGRESQL_PID_FILE=/tmp/master.pid
  postgresql_initialize
}

start_postgres_replica() {
  POSTGRESQL_REPLICATION_MODE='slave' 
  POSTGRESQL_PORT_NUMBER=5443
  POSTGRESQL_MASTER_HOST='127.0.0.1'
  POSTGRESQL_MASTER_PORT_NUMBER=5442
  POSTGRESQL_DATA_DIR=/tmp/postgres-slave
  POSTGRESQL_CONF_DIR=/tmp/postgres-slave-conf
  POSTGRESQL_PID_FILE=/tmp/replica.pid
  postgresql_initialize
}

crash_and_restart_postgres_replica() {
  POSTGRESQL_PORT_NUMBER=5443
  POSTGRESQL_DATA_DIR=/tmp/postgres-slave
  POSTGRESQL_CONF_DIR=/tmp/postgres-slave-conf
  POSTGRESQL_PID_FILE=/tmp/replica.pid
  crash_and_restart_postgres
}

crash_and_restart_postgres_master() {
  POSTGRESQL_PORT_NUMBER=5442
  POSTGRESQL_DATA_DIR=/tmp/postgres-master
  POSTGRESQL_CONF_DIR=/tmp/postgres-master-conf
  POSTGRESQL_PID_FILE=/tmp/master.pid
  crash_and_restart_postgres
}

crash_and_restart_postgres() {
  POSTGRESQL_BIN_DIR=$(pg_config --bindir)
  POSTGRESQL_PGHBA_FILE="$POSTGRESQL_CONF_DIR/pg_hba.conf"
  POSTGRESQL_CONF_FILE="$POSTGRESQL_CONF_DIR/postgresql.conf"
  POSTGRESQL_LOG_FILE="$POSTGRESQL_CONF_DIR/pg.log"
  POSTGRESQL_PGCTLTIMEOUT=60
  POSTGRESQL_DAEMON_USER=postgres
  POSTGRESQL_INIT_MAX_TIMEOUT=60

  pid="$(get_pid_from_file "$POSTGRESQL_PID_FILE")"
  if [[ "$pid" != "" ]]; then
    kill -9 $pid
  fi
  
  # Be sure that postgres is stopped before starting again
  pg_ctl stop -D $POSTGRESQL_DATA_DIR || true
  
  postgresql_start_bg
}

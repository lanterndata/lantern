# this script runs psql in a tmux pane and attaches gdb to the corresponding
# backend process in a separate pane
# the script requires tmux and libtmux
# currently assumes the script was run in an active tmux session
import libtmux
import signal
import subprocess
import re
import os
import getpass
import sys
from select import select
import time
import argparse

sql_common_script_path = os.path.join(os.path.dirname(__file__), "../test/sql/utils/common.sql")
default_user = getpass.getuser()

# helper functions
def get_tmux_session_name() -> str:
    try:
        # Get the current tmux session name
        tmux_session_name_cmd = "tmux display-message -p '#S'"
        tmux_session_name_process = subprocess.Popen(tmux_session_name_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tmux_session_name_output, tmux_session_name_error = tmux_session_name_process.communicate()
        tmux_session_name_output = tmux_session_name_output.decode().strip()
        tmux_session_name_error = tmux_session_name_error.decode().strip()
        return tmux_session_name_output if not tmux_session_name_error else None
    except Exception as e:
        print(f"Error getting current tmux session info: {e}")
        print("Note that livedebug must be run from an active tmux session")
        return None

def livedebug():
    parser = argparse.ArgumentParser(prog='livedebug',
                    description='Attaches gdb to postgres backend process for live debugging')
    parser.add_argument("-p", "--port",  default="5432", help="Port number")
    parser.add_argument("-U", "--user",  default=default_user, help="Database user")
    parser.add_argument("--usepgvector",  action='store_true', help="Initialize pgvector extension when resetting db")
    parser.add_argument("-H", "--host", default="localhost", help="Host name")
    parser.add_argument("--db", default="testdb", help="Database name", )
    parser.add_argument("-f", "--file", default=None, help="SQL file to execute in psql", )
    parser.add_argument('--resetdb', action='store_true', help="Drop and recreate the database before proceeeding")
    args, unknown = parser.parse_known_args()
    if len(unknown) > 0:
        print("Unknown arguments: ", unknown)
        parser.print_help()
        return
    s: libtmux.Server = libtmux.Server()

    default_session: libtmux.session.Session = s.sessions.filter(session_name=get_tmux_session_name()).get()
    python_pane = default_session.attached_pane
    new_pane = python_pane.split_window(False, True, ".", 50)

    if args.resetdb:
        res = subprocess.run(f"psql postgres -U {args.user} -c 'DROP DATABASE IF EXISTS {args.db};'", shell=True)
        res = subprocess.run(f"psql postgres -U {args.user} -c 'CREATE DATABASE {args.db};'", shell=True)
        if args.usepgvector:
            res = subprocess.run(f"psql postgres -U {args.user} -c 'CREATE EXTENSION vector;' -d {args.db}", shell=True)
        res = subprocess.run(f"psql postgres -U {args.user} -c 'CREATE EXTENSION lanterndb;' -d {args.db}", shell=True)
        res = subprocess.run(f"psql postgres -U {args.user} -f {sql_common_script_path} -d {args.db}", shell=True)
        print("resetdb result", res)

    # 1. run the command through a shell
    psql_command = f"psql -U {args.user} -P pager=off -p {args.port} {args.db}"
    psql_process = subprocess.Popen(psql_command, shell=True, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)

    # 2. forward terminal signals to psql
    signal.signal(signal.SIGINT, lambda sig, frame: psql_process.send_signal(sig))
    signal.signal(signal.SIGTERM, lambda sig, frame: psql_process.send_signal(sig))

    backend_pid = None
    # 3. figure out psql backend pid via tmux
    pg_backend_pid_cmd = "select pg_backend_pid();\n"
    python_pane.send_keys(pg_backend_pid_cmd)
    # give time for psql command to complete!
    time.sleep(0.1)

    backend_pid_output = python_pane.capture_pane(-200, '-')
    for i, l in enumerate(reversed(backend_pid_output)):
        if i < 10:
            new_pane.send_keys("echo debug %d '%s'" % (i, l), enter=True)
        try:
            backend_pid_match = re.match(r"\s*(\d+)\s*", l)
            if backend_pid_match:
                backend_pid = int(backend_pid_match.group(1))
                # new_pane.send_keys("echo 'found it' %d " % backend_pid, enter=True)
                break
        except Exception as e:
            print ("psql backend pid extract exception", e)
            psql_process.kill()
            return

    if backend_pid is None:
        print("unable to extract backend pid from psql for live debugging..")
        print(backend_pid_output)
        psql_process.kill()
        return

    # 4. Attach gdb on the detected psql backend pid
    # new_pane.clear()
    new_pane.send_keys("echo attaching gdb to %s" % backend_pid, enter=True)
    new_pane.send_keys("sudo gdb attach -p {}".format(backend_pid), enter=True)

    # 5. load sql file in psql pane if a file was provided
    time.sleep(1.3)
    if args.file:
        python_pane.send_keys(f"\ir {args.file}")

    # 6. wait for psql exit
    psql_process.wait()

if __name__ == "__main__":
    livedebug()



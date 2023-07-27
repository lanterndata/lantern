# this script runs psql in a tmux pane and attaches gdb to the corresponding
# backend process in a separate pane
# the script requires tmux and libtmux
# currently assumes the script was run in an active tmux session
import libtmux
import signal
import subprocess
import re
import sys
from select import select
import time

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
    s: libtmux.Server = libtmux.Server()

    default_session: libtmux.session.Session = s.sessions.filter(session_name=get_tmux_session_name()).get()
    python_pane = default_session.attached_pane
    new_pane = python_pane.split_window(False, True, ".", 50)

    # 1. run the command /usr/local/pgsql/bin/psql -p 4444 -h localhost postgres through a bash shell
    psql_command = "/usr/local/pgsql/bin/psql -P pager=off -p 4444 -h localhost postgres"
    psql_process = subprocess.Popen(psql_command, shell=True, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)

    # forward signals to psql
    signal.signal(signal.SIGINT, lambda sig, frame: psql_process.send_signal(sig))
    signal.signal(signal.SIGTERM, lambda sig, frame: psql_process.send_signal(sig))

    backend_pid = None
    # figure out psql backend pid via tmux
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

    # 4. run new_pane.cmd("echo 'pid is ' + pid")
    # new_pane.clear()
    new_pane.send_keys("echo attaching gdb to %s" % backend_pid, enter=True)
    new_pane.send_keys("sudo gdb attach -p {}".format(backend_pid), enter=True)

    # wait for psql exit
    psql_process.wait()

if __name__ == "__main__":
    livedebug()



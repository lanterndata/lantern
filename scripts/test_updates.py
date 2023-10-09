import argparse
import subprocess
import getpass
import git
import os

def update_from_tag(from_version: str, to_version: str):
    from_tag = "v" + from_version
    to_tag = "v" + to_version
    repo = git.Repo(search_parent_directories=True)
    sha_before = repo.head.object.hexsha
    print("sha_before", sha_before)
    print("checkout to tag", from_tag)
    repo.remotes[0].fetch()
    repo.git.checkout(from_tag)
    sha_after = repo.head.object.hexsha
    print("sha_after", sha_after)

    # run "mkdir build && cd build && cmake .. && make -j4 && make install"
    res = subprocess.run(f"mkdir -p {args.builddir} ; cd {args.builddir} && git submodule update && cmake .. && make -j4 && make install", shell=True)
    if res.returncode != 0:
        if res.stderr:
            print("Error building from tag" + res.stderr)
        print("res stdout", res.stdout, res.stderr, res)
        exit(1)

    res = subprocess.run(f"psql postgres -U {args.user} -c 'DROP DATABASE IF EXISTS {args.db};'", shell=True)
    res = subprocess.run(f"psql postgres -U {args.user} -c 'CREATE DATABASE {args.db};'", shell=True)
    res = subprocess.run(f"psql postgres -U {args.user} -c 'DROP EXTENSION IF EXISTS lantern CASCADE; CREATE EXTENSION lantern;' -d {args.db};", shell=True)
    # todo:: run init() portion of parallel tests

    repo.git.checkout(sha_before)
    print("sha_before", sha_before)
    res = subprocess.run(f"cd {args.builddir} ; git submodule update && cmake .. && make -j4 && make install && make test", shell=True)
    res = subprocess.run(f"cd {args.builddir} ; UPDATE_EXTENSION=1 UPDATE_FROM={from_version} UPDATE_TO={to_version} make test", shell=True)
    #todo:: run query and check portion of parallel tests

if __name__ == "__main__":

    default_user = getpass.getuser()

    # collect the tag from command line to upgrade from

    parser = argparse.ArgumentParser(description='Update from tag')
    parser.add_argument('-from_tag', '--from_tag', metavar='from_tag', type=str,
                        help='Tag to update from', required=False)
    parser.add_argument('-to_tag','--to_tag', metavar='to_tag', type=str,
                        help='Tag to update to', required=False)
    parser.add_argument("-db", "--db", default="update_db", type=str, help="Database name used for updates")
    parser.add_argument("-U", "--user",  default=default_user, help="Database user")
    parser.add_argument("-builddir", "--builddir",  default="build_updates", help="Database user")

    args = parser.parse_args()

    from_tag = args.from_tag
    to_tag = args.to_tag
    if from_tag and to_tag:
        update_from_tag(from_tag, to_tag)

    if from_tag or to_tag:
        print("Must specify both or neither from_tag and to_tag")
        exit(1)

    # test updates from all tags
    print([update_fname for update_fname in os.listdir("sql/updates")])
    from_tags = [update_fname.split("--")[0] for update_fname in os.listdir("sql/updates")]
    print(from_tags)
    latest_version = "latest"
    for from_tag in from_tags:
        update_from_tag(from_tag, latest_version)







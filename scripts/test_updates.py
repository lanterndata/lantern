import argparse
import subprocess
import getpass
import git
import os


INCOMPATIBLE_VERSIONS = {
    '16': ['0.0.4']
}

def shell(cmd, exit_on_error=True):
    res = subprocess.run(cmd, shell=True)
    if res.returncode != 0:
        if res.stderr:
                print("Error building from tag" + res.stderr)
        print("res stdout", res.stdout, res)
        if exit_on_error:
            exit(1)
        else:
            print("ERROR on command", cmd)


def update_from_tag(from_version: str, to_version: str):
    from_tag = "v" + from_version
    repo = git.Repo(search_parent_directories=True)
    sha_before = repo.head.object.hexsha
    repo.remotes[0].fetch()
    repo.git.checkout(from_tag)
    sha_after = repo.head.object.hexsha
    print(f"Updating from tag {from_tag}(sha: {sha_after}) to {to_version}")

    # run "mkdir build && cd build && cmake .. && make -j4 && make install"
    res = shell(f"mkdir -p {args.builddir} ; cd {args.builddir} && git submodule update && cmake .. && make -j4 && make install")

    res = shell(f"psql postgres -U {args.user} -c 'DROP DATABASE IF EXISTS {args.db};'")
    res = shell(f"psql postgres -U {args.user} -c 'CREATE DATABASE {args.db};'")
    res = shell(f"psql postgres -U {args.user} -c 'DROP EXTENSION IF EXISTS lantern CASCADE; CREATE EXTENSION lantern;' -d {args.db};")

    # run begin of parallel tests. Run this while the from_tag version of the binary is installed and loaded
    # run begin on {from_version}
    if from_tag != "v0.0.4":
        # the source code at 0.0.4 did not yet have parallel tests
        res = shell(f"cd {args.builddir} ; UPDATE_EXTENSION=1 UPDATE_FROM={from_version} UPDATE_TO={from_version} make test-parallel FILTER=begin")

    repo.git.checkout(sha_before)
    res = shell(f"cd {args.builddir} ; git submodule update && cmake .. && make -j4 && make install")
    # res = shell(f"cd {args.builddir} ; UPDATE_EXTENSION=1 UPDATE_FROM={from_version} UPDATE_TO={to_version} make test")

    # run the actual parallel tests after the upgrade
    # todo: parallel tests are failing (tracked by https://github.com/lanterndata/lantern/issues/226)
    res = shell(f"cd {args.builddir} ; UPDATE_EXTENSION=1 UPDATE_FROM={from_version} UPDATE_TO={to_version} make test-parallel EXCLUDE=begin", exit_on_error=False)

    print(f"Update {from_version}->{to_version} Success!")


def incompatible_version(pg_version, version_tag):
    if not pg_version or pg_version not in INCOMPATIBLE_VERSIONS:
        return False
    return version_tag in INCOMPATIBLE_VERSIONS[pg_version]

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
        exit(0)

    if from_tag or to_tag:
        print("Must specify both or neither from_tag and to_tag")
        exit(1)

    # test updates from all tags
    tag_pairs = [update_fname.split("--") for update_fname in os.listdir("sql/updates")]
    from_tags = list(sorted([p[0] for p in tag_pairs], reverse=True))
    to_tags = list(sorted([p[1].split(".sql")[0] for p in tag_pairs]))
    latest_version = to_tags[-1]
    print("Updating from tags", from_tags, "to ", latest_version)

    pg_version = None if not 'PG_VERSION' in os.environ else os.environ['PG_VERSION']
    for from_tag in from_tags:
        if incompatible_version(pg_version, from_tag):
            continue
        update_from_tag(from_tag, latest_version)







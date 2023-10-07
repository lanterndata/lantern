import argparse
import subprocess

# collect the tag from command line to upgrade from 

parser = argparse.ArgumentParser(description='Update from tag')
parser.add_argument('tag', metavar='tag', type=str,
                    help='tag to update from')
parser.add_argument("-db", "--db", default="update_db", type=str, help="Database name used for updates")

args = parser.parse_args()
tag = args.tag

# write the code for git checkout to the tag
# use gitPython to checkout to the tag

import git
repo = git.Repo(search_parent_directories=True)
sha_before = repo.head.object.hexsha
print("sha_before", sha_before)
print("checkout to tag", tag)
repo.git.checkout(tag)
sha_after = repo.head.object.hexsha
print("sha_after", sha_after)

# run "mkdir build && cd build && cmake .. && make -j4 && make install"
res = subprocess.run(f"mkdir build && cd build && cmake .. && make -j4 && make install", shell=True)
if res.returncode != 0:
    print("Error building from tag" + res.stderr)
    print("res stdout", res.stdout)
    exit(1)

res = subprocess.run(f"psql postgres -U {args.user} -c 'DROP DATABASE IF EXISTS {args.db};'", shell=True)
res = subprocess.run(f"psql postgres -U {args.user} -c 'CREATE DATABASE {args.db};'", shell=True)
res = subprocess.run(f"psql postgres -c 'CREATE EXTENSION lantern;' -d {args.db};", shell=True)
res = subprocess.run(f"mkdir build && cd build && cmake .. && make -j4 && make install", shell=True)
repo.git.checkout(sha_before)







## Overview

This is a ruby program that uses postgres internal catalog table invariants
to exhaustively verify correctness of Postgres database extension update scripts.

One can install an extension at version $newversion in many ways. Two of those are the ones below:

1. `CREATE EXTENSION ... VERSION $newversion`
2. `CREATE EXTENSION ... VERSION $oldversion; ALTER EXTENSION ... UPDATE TO $newversion`

The first one uses the latest sql catalog script. this usually is extensively tested in regression tests. The second one uses the old catalog script and a carefully
cradted "diff" that is to apply cleanly to the old catalog sql script and bring it up to date. The second option is how changes get deployed to production but the correctness
of the diff script is harder to test.

But, if the diff script is written correctly, the two options above should put the database in equivalent states. This program checks that invariant for a given extension

USAGE

```bash
ruby main.rb [extension_name] [old_version] [new_version]
```

If no `extension_name` is given, all extensions available via `SELECT pg_available_extensions()` are checked.
If no version is given, all extension versions available via `SELECT * FROM pg_available_extension_versions()` are checked.

When the script finds a discrepency between database states obtained via paths 1 and 2, it prints out a concise diff of the two states, involving relevant tables
For example, below is the output of the program when certain permissions are granted on the main sql catalog script of the extension but are not copied to the corresponding update
script:

```
 "PG::PgNamespace"=>
  {:table_name=>"pg_namespace",
   :count_diff=>0,
   :all_diff=>
    [["~",
      "5",
      #<PG::PgNamespace @values={:row_number=>5, :nspname=>"cron", :nspowner=>10, :nspacl=>"{postgres=UC/postgres,=U/postgres}"}>,
      #<PG::PgNamespace @values={:row_number=>5, :nspname=>"cron", :nspowner=>10, :nspacl=>nil}>]],
   :granular_diff=>
    [["~", "[0].nspacl", "{postgres=UC/postgres,=U/postgres}", nil]],
   :num_minus_lines=>1,
   :num_plus_lines=>1},
 "PG::PgClass"=>
  {:table_name=>"pg_class",
   :count_diff=>0,
   :all_diff=>
    [["~",
      "426",
      #<PG::PgClass @values={:row_number=>426, :relname_toastoid_masked=>"tasks_jobid_seq", :reloftype=>0, :relowner=>10, :relam=>0, :reltablespace=>0, :relpages=>1, :reltuples=>1.0, :relallvisible=>0, :relhasindex=>false, :relisshared=>false, :relpersistence=>"p", :relkind=>"S", :relnatts=>3, :relchecks=>0, :relhasrules=>false, :relhastriggers=>false, :relhassubclass=>false, :relrowsecurity=>false, :relforcerowsecurity=>false, :relispopulated=>true, :relreplident=>"n", :relispartition=>false, :relrewrite=>0, :relminmxid=>"0", :relacl=>"{postgres=rwU/postgres,=rU/postgres}", :reloptions=>nil, :relpartbound=>nil}>,
      #<PG::PgClass @values={:row_number=>426, :relname_toastoid_masked=>"tasks_jobid_seq", :reloftype=>0, :relowner=>10, :relam=>0, :reltablespace=>0, :relpages=>1, :reltuples=>1.0, :relallvisible=>0, :relhasindex=>false, :relisshared=>false, :relpersistence=>"p", :relkind=>"S", :relnatts=>3, :relchecks=>0, :relhasrules=>false, :relhastriggers=>false, :relhassubclass=>false, :relrowsecurity=>false, :relforcerowsecurity=>false, :relispopulated=>true, :relreplident=>"n", :relispartition=>false, :relrewrite=>0, :relminmxid=>"0", :relacl=>nil, :reloptions=>nil, :relpartbound=>nil}>],
     ["~",
      "427",
      #<PG::PgClass @values={:row_number=>427, :relname_toastoid_masked=>"tasks", :reloftype=>0, :relowner=>10, :relam=>2, :reltablespace=>0, :relpages=>0, :reltuples=>-1.0, :relallvisible=>0, :relhasindex=>true, :relisshared=>false, :relpersistence=>"p", :relkind=>"r", :relnatts=>9, :relchecks=>0, :relhasrules=>false, :relhastriggers=>false, :relhassubclass=>false, :relrowsecurity=>true, :relforcerowsecurity=>false, :relispopulated=>true, :relreplident=>"d", :relispartition=>false, :relrewrite=>0, :relminmxid=>"1", :relacl=>"{postgres=arwdDxt/postgres,=arwd/postgres}", :reloptions=>nil, :relpartbound=>nil}>,
      #<PG::PgClass @values={:row_number=>427, :relname_toastoid_masked=>"tasks", :reloftype=>0, :relowner=>10, :relam=>2, :reltablespace=>0, :relpages=>0, :reltuples=>-1.0, :relallvisible=>0, :relhasindex=>true, :relisshared=>false, :relpersistence=>"p", :relkind=>"r", :relnatts=>9, :relchecks=>0, :relhasrules=>false, :relhastriggers=>false, :relhassubclass=>false, :relrowsecurity=>true, :relforcerowsecurity=>false, :relispopulated=>true, :relreplident=>"d", :relispartition=>false, :relrewrite=>0, :relminmxid=>"1", :relacl=>"{postgres=arwdDxt/postgres,=r/postgres}", :reloptions=>nil, :relpartbound=>nil}>]],
   :granular_diff=>
    [["~", "[0].relacl", "{postgres=rwU/postgres,=rU/postgres}", nil],
     ["~",
      "[1].relacl",
      "{postgres=arwdDxt/postgres,=arwd/postgres}",
      "{postgres=arwdDxt/postgres,=r/postgres}"]],
   :num_minus_lines=>2,
   :num_plus_lines=>2}}
```

The output is a ruby hash representing the diff of the given catalog table at two points in time: 1) after creating the extension at the new version. 2) After creating the extension at the specified old version and updating it to the new version.
The top-most key is the name of the Sequel model represending the catalog table (`:table_name` is also present in the body).
If in the two paths above the given table had different number of rows, `:count_diff` key will reflect the value.
The next two keys: `:all_diff` and `:granular_diff` give a clue into what exactly is different.
`:all_dff` presents the row objects that are different and `:granular_diff` shows the specific keys inside the object that changed.
Note that many relevant keys (e.g. object oids) are not present in the row data. Those are skipped to avoid sporadic false diffs.
But after a failure you can use `binding.pry` at various points and retrieve more info about the objects causing the diff

## Limitations

- False Positives: The script has been used with Lantern (has found issues in 0.2.3, 0.2.4) and with several extensions shipped with postgres. It has false positives in cases when I have not filtered out oids from a particular table. Somestimes VACUUMing and other DB maintenance also causes sporadic issues. It is easy to patch the script to handle a specific case, however.
- Requires out of Band installation: The script assumes all relevant versions of relevant extensions have been installed
- Assumes updates can happen without server restart: The script assumes that one can CREATE, DROP and ALTER extensions without DB restarts (e.g. extensions that are preloaded and use a custom loader could not be tested with this)

## TODO

- [ ] Enhance various tables with relevant associations and make the diff output more descriptive
- [ ] Add automatic installation script component so it can install extensions from source and test them

## Developing

Things to watch for when developing this further:

- Sequel, sepcially with `eager` and `to_hash` may sometimes return unexpected results. Make sure to inspect the output
- When using non-oid keys in aggregate hash table, make sure that
  whatever is chosen as key is unque (you can also use composite keys)
  Otherwise, two bad things happen: whatever comes first with the key, is
  ignored and the last value having the key is the only one for wich diff
  is computed during the update.
  This means that if the earlier one had a regression, we would not know.
  Second, this causes sporadic failures if the result ordering is not done
  such that to fix the dataset ordering. In different installations
  different records may come last and so in CREATE and ALTER paths
  different values may be the one to win, resulting in apparent regression

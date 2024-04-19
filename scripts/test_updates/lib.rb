require 'sequel'
require 'hashdiff'
require 'pp'
require 'rainbow/refinement'
require 'pry'

using(Rainbow)

TEST_DB_NAME = 'ldb_update_objs_testdb'

# 1000
# Use this to skip large tables if there are irrelevant large tables
# DIFF_THRESHHOLD = 1000

require_relative 'postgres_system'
PG.class_eval do
  def child_classes
    constants
      .collect do |constant|
        c = const_get(constant)
        c if c.is_a?(Class) && c.to_s.start_with?('PG::Pg')
      end
      .compact
  end
end

include(PG)

# filter the ones starting with Pg
def count_and_select_all
  PG
    .child_classes
    .map
    .with_index do |c, _i|
      [c.to_s, { count: c.count, all: c.denormalized_dataset, table_name: c.dataset.first_source_table.to_s }]
    end
    .to_h
end

def tables_diff(old_tables, new_tables)
  ret = new_tables.merge(old_tables) do |_key, new, old|
    # Raise an exception if table names do not match
    raise 'Unreachable' if new[:table_name] != old[:table_name]

    table_name = new[:table_name]

    minus_lines = []
    plus_lines = []
    # Determine the difference based on a threshold
    diff = if defined?(DIFF_THRESHHOLD) && new[:count] > DIFF_THRESHHOLD
             ['LARGE TABLE............. SKIPED!']
           elsif table_name == 'pg_amop'
             # this is a pivot table, diff will not tell anything
             # and if there is a real diff here, it should appear in another table as well
             []
           else
             d = Hashdiff.diff(old[:all], new[:all], ignore_keys: [:oid])
             d.each do |entry|
               raise 'unexpectd diff output' if entry[0].is_a?(Array)

               case entry[0]
               when '-'
                 minus_lines << entry[2]
               when '+'
                 plus_lines << entry[2]
               when '~'
                 minus_lines << entry[2]
                 plus_lines << entry[3]
               else
                 raise "Unexpected diff entry: #{entry}"
               end
             end
             d
           end

    # Return the new hash structure for the merged key
    # binding.pry if minus_lines.length.positive? || plus_lines.length.positive? || diff.length.positive?
    {
      table_name: new[:table_name],
      count_diff: new[:count] - old[:count],
      all_diff: diff,
      # convert Sequel objects to hash so we can percisely print the columns that differ in the differing rows
      granular_diff: Hashdiff.diff(minus_lines.map(&:to_hash), plus_lines.map(&:to_hash)),
      num_minus_lines: minus_lines.length,
      num_plus_lines: plus_lines.length
    }
  end

  ret.select { |_, v| v[:all_diff].length.positive? }
end

def check_update_extension(extension = nil, from_version = nil, to_version = nil)
  if extension.nil?
    all_extensions = DB.fetch(
      "SELECT DISTINCT name FROM pg_available_extension_versions() where name != 'vector' and name != 'plpgsql' and name != 'pg_cron' "
    )
    all_extensions = all_extensions.map { |v| v[:name] }
    puts("Checking all #{all_extensions.count} extensions(#{all_extensions})")
    all_extensions.each { |ext| check_update_extension(ext, from_version, to_version) }
    return
  end

  if from_version.nil? || to_version.nil?
    all_versions = DB
                   .fetch(
                     'SELECT version FROM pg_available_extension_versions() WHERE name = ? ORDER BY version',
                     extension
                   )
                   .all
    all_versions = all_versions.map { |v| v[:version] }.sort_by do |v|
      Gem::Version.new(v)
    rescue StandardError
      Gem::Version.new('100.1.1')
    end

    to_version = all_versions.last if to_version.nil?

    if from_version.nil?
      from_versions = all_versions[0..(all_versions.length - 2)]
      puts("Updating from all versions(#{from_versions.join(', ')}) to #{to_version}")
      from_versions.each do |f|
        check_update_extension(extension, f, to_version)
      end

      return
    end
  end

  puts("Checking extension \"#{extension}\" from '#{from_version}' to '#{to_version}'")

  create_extension_count = nil
  update_extension_count = nil
  DB.run("DROP EXTENSION IF EXISTS \"#{extension}\" CASCADE")
  no_extension_count = count_and_select_all

  DB.transaction do
    DB.rollback_on_exit
    DB.run("CREATE EXTENSION \"#{extension}\" VERSION '#{to_version}'")
    # make function callable by public
    create_extension_count = count_and_select_all
    # upgrade to target version, starting at from_version
    # db.run("DROP EXTENSION IF EXISTS \"#{extension}\" CASCADE")
  end

  DB.transaction do
    DB.rollback_on_exit
    DB.run("CREATE EXTENSION \"#{extension}\" VERSION '#{from_version}'")
    DB.run("ALTER EXTENSION \"#{extension}\" UPDATE TO '#{to_version}'")
    update_extension_count = count_and_select_all
  end

  DB.disconnect
  new_objects = tables_diff(no_extension_count, create_extension_count)
  updated_vs_new_ext_objects = tables_diff(create_extension_count, update_extension_count)

  added_object_summary = new_objects.map { |_k, v| "#{v[:table_name]}(#{v[:count_diff]})" }

  print(
    "\tThe following system tables have updated row counts as a result of installing #{from_version}:\n".green,
    "\t #{added_object_summary.join(', ')}\n"
  )

  return unless updated_vs_new_ext_objects.length.positive?

  updated_object_summary = updated_vs_new_ext_objects.map { |_k, v| "#{v[:table_name]}(#{v[:count_diff]})" }
  print(
    "\tThe following system tables have updated rows as a result of upgrading to #{to_version}: \n".red,
    "\t #{updated_object_summary.join(', ')}\n"
  )
  pp(updated_vs_new_ext_objects)
end

require 'thor'
require 'sequel'
require 'hashdiff'
require 'pp'
require 'rainbow/refinement'
require 'pry'

DB = Sequel.connect(ENV['DATABASE_URL'])
DB.run(
  "CREATE OR REPLACE FUNCTION get_functiondef_or_none(oid oid) RETURNS text AS $$ BEGIN RETURN pg_get_functiondef(oid); EXCEPTION WHEN others THEN RETURN 'NONE'; END; $$ LANGUAGE plpgsql"
)
DB.extension(:select_remove)

require_relative './lib'

using(Rainbow)

class DatabaseCLI < Thor
  def self.exit_on_failure?
    true
  end

  desc 'test EXTENSION_NAME FROM_VERSION TO_VERSION', 'Check and update the specified extension'
  def test(extension_name = nil, from_version = nil, to_version = nil)
    # check_update_extension("lantern", "0.2.4", "0.2.5")
    # check_update_extension("lantern", nil, "0.2.3")
    # check_update_extension('vector', '0.3.0', '0.6.1')
    # check_update_extension
    # check_update_extension('lantern', '0.2.2', '0.2.4')
    # check_update_extension('cube')
    # check_update_extension("postgis")
    # check_update_extension("lantern", "1.2", "1.4")
    # check_update_extension("lantern", nil, "0.2.4")
    # check_update_extension("vector", "0.6.2", "0.7.0")
    # check_update_extension("lantern", "0.2.3", "0.2.4")
    check_update_extension(extension_name, from_version, to_version)
  end
end

DatabaseCLI.start(ARGV)

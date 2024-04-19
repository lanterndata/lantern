require 'sequel'

PG_SERVER_VERSION_NUM = DB.fetch('SHOW server_version_num').first[:server_version_num].to_i

# we need to filter out and not define sequel model classes for system tables that do not exist in this version
# of the database. We need to do it here since Sequel attempts to connect to the table at class definition time
TABLE_AVAILABLE_SINCE = {
  'PG::PgParameterAcl' => 150_000,
  'PG::PgPublicationNamespace' => 150_000,
  'PG::PgStatisticExtData' => 120_000
}

module Sequel::Plugins::DenormalizedDataset
  module ClassMethods
    def ignored_columns
      [:oid]
    end

    def denormalized_dataset
      raise "Custom denormalized_dataset method is not implemented for #{self}" if primary_key.nil?

      dataset.select(Sequel.function(:row_number).over(order: Sequel.asc(primary_key)),
                     *(dataset.columns - ignored_columns)).to_hash(:row_number)
    end
  end
end

Sequel::Model.plugin(:denormalized_dataset)
# Sequel::Model.plugin :SelectRemove

module PG
  class PgAggregate < Sequel::Model(:pg_aggregate)
    set_primary_key :aggfnoid
    def self.ignored_columns
      # TODO: : add eager loading insteadt
      [:aggtranstype]
    end
  end

  class PgAm < Sequel::Model(:pg_am)
    set_primary_key :oid
    one_to_one :oid, class: '::PG::PgClass', key: :relam
    set_primary_key :oid
  end

  class PgAmop < Sequel::Model(:pg_amop)
    set_primary_key :oid
  end

  class PgAmproc < Sequel::Model(:pg_amproc)
    set_primary_key :oid
    def self.ignored_columns
      %i[oid amprocfamily amproclefttype amprocrighttype]
    end
  end

  class PgAttrdef < Sequel::Model(:pg_attrdef)
    set_primary_key :oid
    def self.ignored_columns
      %i[oid adrelid adbin]
    end
  end

  class PgAttribute < Sequel::Model(:pg_attribute)
    set_primary_key %i[attrelid attnum]
    def self.ignored_columns
      [:attrelid]
    end
  end

  class PgAuthid < Sequel::Model(:pg_authid)
    set_primary_key :oid
  end

  class PgAuthMembers < Sequel::Model(:pg_auth_members)
    set_primary_key %i[roleid member]
  end

  class PgCast < Sequel::Model(:pg_cast)
    set_primary_key :oid
    many_to_one :castsource, class: '::PG::PgType', key: :castsource
    many_to_one :casttarget, class: '::PG::PgType', key: :casttarget
    many_to_one :castfunc, class: '::PG::PgProc', key: :castfunc

    def self.ignored_columns
      %i[oid castsource casttarget castfunc]
    end

    def self.denormalized_dataset
      assoc_pg_type_cols = %i[oid typname typinput typoutput typsend typreceive]
      stor = proc { |ds|
        ds.select(*assoc_pg_type_cols)
      }
      res = dataset
            .select(*(dataset.columns - ignored_columns), Sequel.function(:row_number).over(order: Sequel.asc(:oid)).as(:row_number))
            # Sequel.lit("RANDOM() as haha"),
            .eager_graph({ castsource: stor }, { casttarget: stor }, { castfunc: proc { |ds|
                                                                                   ds.select(:oid, :proname)
                                                                                 } })
      # NOTE: not including v in the result because:
      # there is little useful direct info
      # do not want to include the IDs since there is no easy way to ignore oid-drift regressions
      # I cannot filter oid-esque columns above since those are needed for eager_graph
      # I cannot exclude the columns after eager_graph above since select() is private on the object at that point
      res.to_hash(:row_number).map do |k, v|
        # to_hash makes for a much larger comparison set
        Hash[k, [v.castsource.to_hash, v.casttarget.to_hash]]
      end
    end
  end

  class PgClass < Sequel::Model(:pg_class)
    set_primary_key :oid
    one_to_one :indexrel, class: '::PG::PgIndex', key: :indexrelid
    one_to_many :indrel, class: '::PG::PgIndex', key: :indrelid
    one_to_many :indam, class: '::PG::PgAm', key: :oid, primary_key: :relam
    def self.ignored_columns
      # we ignore relname and select a processed version of it that masks OID in toast tables
      %i[oid relfilenode relnamespace relfrozenxid reltoastrelid reltype relname]
    end

    def self.denormalized_dataset
      dataset.select(Sequel.function(:row_number).over(order: Sequel.asc(primary_key)),
                     Sequel.lit("CASE WHEN relname LIKE 'pg_toast%' THEN 'pg_toast' ELSE relname END").as(:relname_toastoid_masked),
                     *(dataset.columns - ignored_columns)).to_hash(:row_number)
    end
  end

  class PgCollation < Sequel::Model(:pg_collation)
    set_primary_key :oid
  end

  class PgConstraint < Sequel::Model(:pg_constraint)
    set_primary_key :oid
    def self.ignored_columns
      %i[oid conindid connamespace conrelid]
    end
  end

  class PgConversion < Sequel::Model(:pg_conversion)
    set_primary_key :oid
  end

  # we ignore changes in this table as it is a table across DBs and does not have stuff relevant to extensions
  # class PgDatabase < Sequel::Model(:pg_database)
  #   set_primary_key :oid
  # end

  class PgDbRoleSetting < Sequel::Model(:pg_db_role_setting)
    set_primary_key %i[setdatabase setrole]
  end

  class PgDefaultAcl < Sequel::Model(:pg_default_acl)
    set_primary_key :oid
  end

  # there are mostly OIDs and it did not seem worth it fixing all the joins for getting their diffs
  # class PgDepend < Sequel::Model(:pg_depend)
  #   set_primary_key [:classid, :objid, :objsubid]
  # end

  # class PgDescription < Sequel::Model(:pg_description)
  #   set_primary_key [:objoid, :classoid, :objsubid]
  #
  #   def self.ignored_columns
  #     return [:objoid, :classoid, :objsubid]
  #   end
  #   def self.denormalized_dataset
  #     return self.dataset.select(Sequel.lit("*"),
  #                               ).all
  #     # .each_with_object({}) do |row, result|
  #     #   result[row[:oid]] = row
  #     #   puts row.to_hash
  #     # end
  #   end
  # end

  class PgEnum < Sequel::Model(:pg_enum)
    set_primary_key :oid
  end

  class PgEventTrigger < Sequel::Model(:pg_event_trigger)
    set_primary_key :oid
  end

  class PgExtension < Sequel::Model(:pg_extension)
    set_primary_key :oid
  end

  class PgForeignDataWrapper < Sequel::Model(:pg_foreign_data_wrapper)
    set_primary_key :oid
  end

  class PgForeignServer < Sequel::Model(:pg_foreign_server)
    set_primary_key :oid
  end

  class PgForeignTable < Sequel::Model(:pg_foreign_table)
    set_primary_key :ftrelid
  end

  class PgIndex < Sequel::Model(:pg_index)
    set_primary_key :indexrelid
    one_to_one :pgclass, class: '::PG::PgClass', key: :oid
    many_to_one :table, class: '::PG::PgClass', key: :indrelid
    one_through_one :am, class: '::PG::PgAm', join_table: 'pg_class', left_key: :oid, right_key: :relam
    def self.ignored_columns
      %i[indexrelid indrelid]
    end

    def validate
      super
      errors.add(:name, 'editting not allowed')
      validates_unique
    end
  end

  class PgInherits < Sequel::Model(:pg_inherits)
    set_primary_key %i[inhrelid inhparent]
  end

  class PgInitPrivs < Sequel::Model(:pg_init_privs)
    set_primary_key %i[objoid classoid objsubid]
    many_to_one :classoid, class: '::PG::PgClass', key: :classoid

    def self.ignored_columns
      %i[objoid classoid objsubid]
    end

    def self.denormalized_dataset
      dataset.select(*(dataset.columns - ignored_columns),
                     Sequel.function(:row_number).over(order: Sequel.asc(primary_key)).as(:row_number))
             .eager_graph(classoid: proc { |ds|
                                      ds.select(:oid, :relname)
                                    })
             .to_hash(:row_number).map do |k, v|
               # TODO: : how could I use Sequel's injection-safe interpolation here to interploate an identifier (table name)
               row = DB.fetch("SELECT * FROM #{v.classoid[:relname]} WHERE oid = ?", v.to_hash[:objoid])
               Hash[k,
                    [v,
                     v.classoid[:relname]]]
             end
    end
  end

  class PgLanguage < Sequel::Model(:pg_language)
    set_primary_key :oid
  end

  class PgLargeobject < Sequel::Model(:pg_largeobject)
    set_primary_key %i[loid pageno]
  end

  class PgLargeobjectMetadata < Sequel::Model(:pg_largeobject_metadata)
    set_primary_key :oid
  end

  class PgNamespace < Sequel::Model(:pg_namespace)
    set_primary_key :oid
  end

  class PgOpclass < Sequel::Model(:pg_opclass)
    set_primary_key :oid
    def self.ignored_columns
      %i[oid opcintype opcfamily opcmethod]
    end
  end

  class PgOperator < Sequel::Model(:pg_operator)
    set_primary_key :oid

    def self.ignored_columns
      []
    end

    def self.denormalized_dataset
      # sort results w.r.t. regproc-function and op name to make the diff more sane
      dataset.select(*(dataset.columns - ignored_columns)).order(:oprcode, :opname)
    end
  end

  class PgOpfamily < Sequel::Model(:pg_opfamily)
    set_primary_key :oid

    def self.ignored_columns
      %i[oid opfmethod]
    end
  end

  if !TABLE_AVAILABLE_SINCE['PG::PgParameterAcl'] || TABLE_AVAILABLE_SINCE['PG::PgParameterAcl'] <= PG_SERVER_VERSION_NUM
    class PgParameterAcl < Sequel::Model(:pg_parameter_acl)
      set_primary_key :oid
    end
  end

  class PgPartitionedTable < Sequel::Model(:pg_partitioned_table)
    set_primary_key :partrelid
  end

  class PgPolicy < Sequel::Model(:pg_policy)
    set_primary_key :oid
    def self.ignored_columns
      %i[oid polrelid]
    end
  end

  class PgProc < Sequel::Model(:pg_proc)
    set_primary_key :oid

    def self.manual_ignored_columns
      # proargdefaults needs to be ignored because of a cryptically changing :location part
      # example:
      # ["~",
      # "[5].proargdefaults",
      # "({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 13047 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]})",
      # "({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 12945 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]})"],
      %i[oid pronamespace prorettype proargtype proargtypes proargdefaults]
    end

    def self.denormalized_dataset
      a = dataset.select(
        # Sequel.function(:row_number).over(order: Sequel.asc(:oid)).as(:row_number),
        *(dataset.columns - manual_ignored_columns),
        Sequel.lit('pg_get_expr(proargdefaults, 0) as arg_defaults_parsed'),
        Sequel.lit('get_functiondef_or_none(oid)').as(:function_definition)
      ).order(:function_definition, :pronargs)
      # Sequel.lit("random() as rand"),
      # select_remove exists but does not work in this context.
      # todo: check if this is one of the places it is supposed to not work
      # https://www.rubydoc.info/github/jeremyevans/sequel/Sequel/SelectRemove
      # .select_remove(:pronamespace)
      a.to_hash(%i[proname pronargs])
    end
  end

  class PgPublication < Sequel::Model(:pg_publication)
    set_primary_key :oid
  end

  if !TABLE_AVAILABLE_SINCE['PG::PgPublicationNamespace'] || TABLE_AVAILABLE_SINCE['PG::PgPublicationNamespace'] <= PG_SERVER_VERSION_NUM
    class PgPublicationNamespace < Sequel::Model(:pg_publication_namespace)
      set_primary_key :oid
    end
  end

  class PgPublicationRel < Sequel::Model(:pg_publication_rel)
    set_primary_key :oid
  end

  class PgRange < Sequel::Model(:pg_range)
    set_primary_key :rngtypid
  end

  class PgReplicationOrigin < Sequel::Model(:pg_replication_origin)
    set_primary_key :roident
  end

  class PgRewrite < Sequel::Model(:pg_rewrite)
    set_primary_key :oid
  end

  class PgSeclabel < Sequel::Model(:pg_seclabel)
    set_primary_key %i[objoid classoid objsubid provider]
  end

  class PgSequence < Sequel::Model(:pg_sequence)
    set_primary_key :seqrelid
    def self.ignored_columns
      [:seqrelid]
    end
  end

  # does not have primary key and is global.
  # not needed rn
  # class PgShdepend < Sequel::Model(:pg_shdepend)
  # end

  class PgShdescription < Sequel::Model(:pg_shdescription)
    set_primary_key %i[objoid classoid]
  end

  class PgShseclabel < Sequel::Model(:pg_shseclabel)
    set_primary_key %i[objoid classoid provider]
  end

  class PgStatistic < Sequel::Model(:pg_statistic)
    set_primary_key %i[starelid staattnum stainherit]
  end

  class PgStatisticExt < Sequel::Model(:pg_statistic_ext)
    set_primary_key :oid
  end

  if !TABLE_AVAILABLE_SINCE['PG::PgStatisticExtData'] || TABLE_AVAILABLE_SINCE['PG::PgStatisticExtData'] <= PG_SERVER_VERSION_NUM
    class PgStatisticExtData < Sequel::Model(:pg_statistic_ext_data)
      # stxdinherit is also part of primary key now, but it was added on PG15
      set_primary_key %i[stxoid]
    end
  end

  class PgSubscription < Sequel::Model(:pg_subscription)
    set_primary_key :oid
  end

  class PgSubscriptionRel < Sequel::Model(:pg_subscription_rel)
    set_primary_key %i[srrelid srsubid]
  end

  class PgTablespace < Sequel::Model(:pg_tablespace)
    set_primary_key :oid
  end

  class PgTransform < Sequel::Model(:pg_transform)
    set_primary_key :oid
  end

  class PgTrigger < Sequel::Model(:pg_trigger)
    set_primary_key :oid
    def self.ignored_columns
      %i[oid tgfoid]
    end
  end

  class PgTsConfig < Sequel::Model(:pg_ts_config)
    set_primary_key :oid
  end

  class PgTsConfigMap < Sequel::Model(:pg_ts_config_map)
    set_primary_key %i[mapcfg maptokentype]
  end

  class PgTsDict < Sequel::Model(:pg_ts_dict)
    set_primary_key :oid
  end

  class PgTsParser < Sequel::Model(:pg_ts_parser)
    set_primary_key :oid
  end

  class PgTsTemplate < Sequel::Model(:pg_ts_template)
    set_primary_key :oid
  end

  class PgType < Sequel::Model(:pg_type)
    set_primary_key :oid
    one_to_many :oid, class: '::PG::PgCast', key: :castsource
    one_to_many :oid, class: '::PG::PgCast', key: :casttarget
    # there is a unique constraint on (typname, typnamespace)
    def self.ignored_columns
      %i[oid typarray typelem typrelid typnamespace typname]
    end

    def self.denormalized_dataset
      ret = dataset.select(Sequel.function(:row_number).over(order: Sequel.asc(:oid)).as(:row_number),
                           Sequel.lit("CASE WHEN typname LIKE 'pg_toast%' THEN 'pg_toast' ELSE typname END").as(:typname_toastoid_masked),
                           *(dataset.columns - ignored_columns))
      ret.to_hash(:row_number) # .map { |k, v| [[k, v.to_hash]].to_h }
    end
  end

  class PgUserMapping < Sequel::Model(:pg_user_mapping)
    set_primary_key :oid
  end
end

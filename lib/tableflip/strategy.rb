require 'ostruct'

class Tableflip::Strategy
  # == Constants ============================================================

  PROPERTIES = {
    array: [
      :actions,
      :tables,
      :exclude_columns,
      :ignore_binary,
      :on_duplicate_max
    ]
  }.freeze

  # == Properties ===========================================================

  PROPERTIES[:array].each do |name|
    attr_reader name
  end

  attr_accessor :block_size
  attr_accessor :config_path
  attr_accessor :dry_run
  attr_accessor :debug_queries
  attr_accessor :encoding
  attr_accessor :fuzz_intensity
  attr_accessor :message
  attr_accessor :migrate_method
  attr_accessor :persist
  attr_accessor :source_env
  attr_accessor :target_env
  attr_accessor :where

  # == Class Methods ========================================================

  # == Instance Methods =====================================================

  def initialize
    PROPERTIES[:array].each do |name|
      instance_variable_set(:"@#{name}", [ ])
    end

    @dry_run = false
    @fuzz_intensity = 1
    @block_size = 10000
    @migrate_method = :replace

    @source_env = 'source'.freeze
    @target_env = 'target'.freeze

    yield(self) if (block_given?)
  end

  def merge(strategy)
    PROPERTIES[:array].each do |name|
      var = :"@#{name}"

      instance_variable_set(
        var,
        (instance_variable_get(var) + strategy.instance_variable_get(var)).uniq
      )
    end

    self
  end

  def persist?
    !!@persist
  end

  def complete?
    !!@complete
  end

  def debug_queries?
    !!@debug_queries
  end

  def table_matched?(table)
    tables_matched([ table ]).any?
  end

  def tables_matched(list)
    list.map(&:to_s).grep(self.table_regexp).reject do |table|
      case (table)
      when 'schema_migrations', 'schema_info', /__changes\z/
        true
      end
    end.map(&:to_sym)
  end

  def table_regexp
    Regexp.new([
      '\A(?:',
      Regexp.union(
        @tables.map do |table|
          Regexp.new(table.to_s.gsub(/\*/, '.*'))
        end
      ),
      ')\z'
    ].join(''))
  end

  def dup
    copy = super

    PROPERTIES[:array].each do |name|
      var = :"@#{name}"
      copy.instance_variable_set(var, instance_variable_get(var).dup)
    end

    copy
  end
end

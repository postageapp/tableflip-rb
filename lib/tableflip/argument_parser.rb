require 'optparse'
require 'yaml'

class Tableflip::ArgumentParser
  # == Constants ============================================================

  TABLE_SPECIFIC_STRATEGY = '-g'
  TABLE_SPECIFIC_STRATEGY_FILE_NAME = 'table_migrating_strategies.yml'

  # == Class Methods ========================================================

  def self.default_env(env = nil)
    (env || ENV)['RAILS_ENV'] || 'development'
  end

  # == Instance Methods =====================================================

  def initialize
  end

  def parse(args, env = nil)
    strategy = Tableflip::Strategy.new

    strategy.source_env = self.class.default_env(env)

    # Strip off all other commands if using specific table strategy
    if args.include?(TABLE_SPECIFIC_STRATEGY)
      get_option_index = args.index(TABLE_SPECIFIC_STRATEGY)
      args = [TABLE_SPECIFIC_STRATEGY, args[get_option_index + 1]]
    end

    _parser = parser(strategy)

    tables = _parser.parse!(args)

    tables.each do |table|
      strategy.tables << table
    end

    if (strategy.tables.empty? or strategy.actions.empty?)
      strategy.message = _parser.to_s
    end

    strategy
  end

  def parser(strategy)
    OptionParser.new do |parser|
      parser.banner = "Usage: tableflip [options] [table_name [table_name [...]]]"

      parser.separator("")
      parser.separator("Options:")

      parser.on("-a", "--all", "Track all tables") do |s|
        strategy.tables << :__all__
      end
      parser.on("-i", "--insert", "Use INSERT IGNORE instead of REPLACE INTO") do
        strategy.migrate_method = :insert
      end
      parser.on("-b", "--block=s", "Transfer data in blocks of N rows") do |s|
        strategy.block_size = s.to_i
      end
      parser.on("-f", "--config=s") do |path|
        strategy.config_path = path
      end
      parser.on("-t", "--track", "Add tracking triggers on tables") do
        strategy.actions << :tracking_add
      end
      parser.on("-d", "--seed", "Seed the tracking table with entries from the source table") do
        strategy.actions << :tracking_seed
      end
      parser.on("-r", "--remove", "Remove tracking triggers from tables") do
        strategy.actions << :tracking_remove
      end
      parser.on("-o", "--target=s", "Set target environment") do |s|
        strategy.target_env = s
      end
      parser.on("-m", "--migrate", "Migrate data from source to target") do
        strategy.actions << :table_migrate
      end
      parser.on("-c", "--count", "Count number of records in source table") do
        strategy.actions << :table_count
      end
      parser.on("-s", "--status", "Show current status") do
        strategy.actions << :table_report_status
      end
      parser.on("-e", "--env=s", "Establish primary environment") do |s|
        strategy.source_env = s
      end
      parser.on('--ignore-binary=s', 'Ignore binary encoding requirement for column') do |s|
        strategy.ignore_binary << s.to_sym
      end
      parser.on("-x", "--exclude=s", "Exclude column(s) from migration") do |s|
        s.split(/,/).each do |column|
          strategy.exclude_columns << column
        end
      end
      parser.on("-n", "--encoding=s", "Set connection encoding") do |s|
        strategy.encoding = s
      end
      parser.on("-k", "--create-test", "Creates a test table") do
        strategy.actions << :table_create_test
      end
      parser.on("-z", "--fuzz[=d]", "Inserts and alters records on test table") do |d|
        strategy.actions << :table_fuzz

        if (d)
          strategy.fuzz_intensity = d.to_i
        end
      end
      parser.on("-p", "--persist", "Keep running perpetually") do
        strategy.persist = true
      end
      parser.on("-w","--where=s", "Add conditions to selecting") do |s|
        strategy.where = s
      end
      parser.on("-q", "--debug", "Show the queries as they're executed") do
        strategy.debug_queries = true
      end
      parser.on("-h", "--help", "Display this help") do
        strategy.message = parser.to_s
      end
      parser.on("-g", "--table-strategy=s", "Specifc table migrating strategy") do |s|
        load_file = File.open(
          File.expand_path(File.join('..', '..', 'config', TABLE_SPECIFIC_STRATEGY_FILE_NAME), File.dirname(__FILE__))
        )

        table_strategy = YAML.load(load_file)

        if table_strategy.keys.include?(s)
          loaded_strategy = table_strategy[s]

          args = loaded_strategy.map do |ele|
            case ele
            when Hash
              key = ele.keys[0]
              value = ele[key]
              "--%s=%s" %[key, value]
            when String
              "--%s" % ele
            end
          end << s

          _parser = parser(strategy)
          _parser.parse!(args)

          strategy.tables << s
        end
      end
    end
  end
end

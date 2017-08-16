require 'optparse'
require 'yaml'

class Tableflip::ArgumentParser
  # == Constants ============================================================

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

    _parser = parser(strategy)

    args = _parser.parse!(args)

    strategies = [ base_strategy ]

    args.each do |arg|
      case (arg)
      when /\.ya?ml\z/
        strategies += merge_from_file(arg)
      else
        base_strategy.tables << table
      end
    end

    if (base_strategy.tables.empty? or base_strategy.actions.empty?)
      base_strategy.message = _parser.to_s
    end

    strategies
  end

  def merge_from_file(path)
    config = YAML.load(File.open(path))

    config['tables'].map do |table, table_config|
      Tableflip::Strategy.new do |strategy|
        strategy.tables << table.to_sym

        if (table_config['track'])
          strategy.actions << :tracking_add
        end
        if (table_config['seed'])
          strategy.actions << :tracking_seed
        end
        if (table_config['migrate'])
          strategy.actions << :tracking_migrate
        end
      end
    end
  end

  def parser(strategy)
    OptionParser.new do |parser|
      parser.banner = 'Usage: tableflip [options] [table_name|config_file [table_name|config_file [...]]]'

      parser.separator('')
      parser.separator('Options:')

      parser.on('-a', '--all', 'Track all tables') do |s|
        strategy.tables << :*
      end
      parser.on('--force-insert', 'Use INSERT IGNORE instead of REPLACE INTO') do
        strategy.migrate_method = :insert
      end
      parser.on('-b', '--block=s', 'Transfer data in blocks of N rows') do |s|
        strategy.block_size = s.to_i
      end
      parser.on('-f', '--config=s', 'Path to the database config') do |path|
        strategy.config_path = path
      end
      parser.on('-t', '--track', 'Add tracking triggers on tables') do
        strategy.actions << :tracking_add
      end
      parser.on('-d', '--seed', 'Seed the tracking table with entries from the source table') do
        strategy.actions << :tracking_seed
      end
      parser.on('-r', '--remove', 'Remove tracking triggers from tables') do
        strategy.actions << :tracking_remove
      end
      parser.on('-o', '--target=s', 'Set target environment') do |s|
        strategy.target_env = s
      end
      parser.on('-m', '--migrate', 'Migrate data from source to target') do
        strategy.actions << :table_migrate
      end
      parser.on('-c', '--count', 'Count number of records in source table') do
        strategy.actions << :table_count
      end
      parser.on('-s', '--status', 'Show current status') do
        strategy.actions << :table_report_status
      end
      parser.on('-e', '--env=s', 'Establish primary environment') do |s|
        strategy.source_env = s
      end
      parser.on('--ignore-binary=s', 'Ignore binary encoding requirement for column') do |s|
        strategy.ignore_binary << s.to_sym
      end
      parser.on('--on-duplicate-max=s', 'On key collision use MAX(VALUE(x)) instead of x') do |s|
        strategy.on_duplicate_max << s.to_sym
      end
      parser.on('-x', '--exclude=s', 'Exclude column(s) from migration') do |s|
        s.split(/,/).each do |column|
          strategy.exclude_columns << column
        end
      end
      parser.on('-n', '--encoding=s', 'Set connection encoding') do |s|
        strategy.encoding = s
      end
      parser.on('-k', '--create-test', 'Creates a test table') do
        strategy.actions << :table_create_test
      end
      parser.on('-z', '--fuzz[=d]', 'Inserts and alters records on test table') do |d|
        strategy.actions << :table_fuzz

        if (d)
          strategy.fuzz_intensity = d.to_i
        end
      end
      parser.on('-p', '--persist', 'Keep running perpetually') do
        strategy.persist = true
      end
      parser.on('-w','--where=s', 'Add conditions to selecting') do |s|
        strategy.where = s
      end
      parser.on('-q', '--debug', 'Show the queries being executed') do
        strategy.debug_queries = true
      end
      parser.on('-y', '--dry-run', 'Show the execution plan without actually running it') do
        strategy.dry_run = true
      end
      parser.on('-h', '--help', 'Display this help') do
        strategy.message = parser.to_s
      end
    end
  end
end

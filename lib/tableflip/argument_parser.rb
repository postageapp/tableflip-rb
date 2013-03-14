require 'optparse'

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
      parser.banner = "Usage: tableflip [options] table_name [table_name [...]]"

      parser.separator("")
      parser.separator("Options:")

      parser.on("-c", "--config=s") do |path|
        strategy.config_path = path
      end
      parser.on("-t", "--track", "Add tracking triggers on tables") do
        strategy.actions << :track
      end
      parser.on("-m", "--migrate=s", "Migrate tables to environment") do |s|
        strategy.actions << :migrate
        strategy.target_env = s
      end
      parser.on("-c", "--count", "Count number of records in source table") do
        strategy.actions << :count
      end
      parser.on("-p", "--pending", "Count number of pending changes") do
        strategy.actions << :pending
      end
      parser.on("-e", "--env=s", "Establish primary environment") do
        strategy.source_env = s
      end
      parser.on("-r", "--remove", "Remove tracking triggers from tables") do
        strategy.actions << :remove
      end
      parser.on("-h", "--help", "Display this help") do
        strategy.message = parser.to_s
      end
    end
  end
end

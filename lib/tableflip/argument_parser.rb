require 'optparse'

class Tableflip::ArgumentParser
  # == Extensions ===========================================================
  
  # == Constants ============================================================

  # == Properties ===========================================================

  # == Class Methods ========================================================
  
  # == Instance Methods =====================================================

  def initialize
  end

  def parse(args)
    strategy = Tableflip::Strategy.new

    _parser = parser(strategy)

    strategy.tables += _parser.parse!(args)

    if (strategy.tables.empty? or strategy.actions.empty?)
      strategy.message = _parser.to_s
    end

    strategy
  end

  def parser(strategy)
    OptionParser.new do |parser|
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
      parser.on("-e", "--env=s", "Establish primary environment") do
        strategy.source_env = s
      end
      parser.on("-r", "--remove", "Remove tracking triggers from tables") do
        strategy.actions << :remove
      end
      parser.on("-h", "--help") do
        strategy.message = parser.to_s
      end
    end
  end
end

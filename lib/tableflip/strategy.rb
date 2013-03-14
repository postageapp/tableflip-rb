require 'ostruct'

class Tableflip::Strategy
  # == Properties ===========================================================

  attr_accessor :actions
  attr_accessor :config_path
  attr_accessor :source_env
  attr_accessor :tables
  attr_accessor :target_env
  attr_accessor :message
  attr_accessor :complete
  attr_accessor :fuzz_intensity
  attr_accessor :block_size
  attr_accessor :persist

  # == Class Methods ========================================================

  # == Instance Methods =====================================================

  def initialize
    @actions = [ ]
    @tables = [ ]
    @fuzz_intensity = 1
    @block_size = 1000

    yield(self) if (block_given?)
  end

  def persist?
    !!@persist
  end

  def complete?
    !!@complete
  end
end

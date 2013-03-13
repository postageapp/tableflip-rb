require 'ostruct'

class Tableflip::Strategy
  # == Properties ===========================================================

  attr_accessor :actions
  attr_accessor :config_path
  attr_accessor :source_env
  attr_accessor :tables
  attr_accessor :target_env
  attr_accessor :message
  
  # == Class Methods ========================================================

  # == Instance Methods =====================================================

  def initialize
    @actions = [ ]
    @tables = [ ]

    yield(self) if (block_given?)
  end
end

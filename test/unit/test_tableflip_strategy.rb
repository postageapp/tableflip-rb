require_relative '../helper'

class TestTableflipStrategy < Test::Unit::TestCase
  def test_defaults
    strategy = Tableflip::Strategy.new

    assert_equal [ ], strategy.actions
    assert_equal nil, strategy.config_path
    assert_equal nil, strategy.source_env
    assert_equal [ ], strategy.tables
    assert_equal nil, strategy.target_env
  end
end

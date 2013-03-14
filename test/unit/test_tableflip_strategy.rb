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

  def test_example
    strategy = Tableflip::Strategy.new do |strategy|
      strategy.actions << :test

      strategy.tables << :table_a
      strategy.tables << :table_b

      strategy.source_env = 'staging'
      strategy.target_env = 'test'
    end

    assert_equal [ :test ], strategy.actions
    assert_equal [ :table_a, :table_b ], strategy.tables
    assert_equal 'staging', strategy.source_env
    assert_equal 'test', strategy.target_env

    assert_equal nil, strategy.message
  end
end

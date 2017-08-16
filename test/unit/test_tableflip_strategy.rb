require_relative '../helper'

class TestTableflipStrategy < Test::Unit::TestCase
  def test_defaults
    strategy = Tableflip::Strategy.new

    assert_equal [ ], strategy.actions
    assert_equal nil, strategy.config_path
    assert_equal [ ], strategy.tables

    assert_equal 'source', strategy.source_env
    assert_equal 'target', strategy.target_env
  end

  def test_with_options
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

  def test_table_wildcard_patterns
    examples = [
      :table_a,
      :table_b1,
      :table_b2,
      :table_b12,
      :table_x,
      :final
    ].freeze

    strategy = Tableflip::Strategy.new do |strategy|
      strategy.tables << :*
    end

    assert_equal examples, strategy.tables_matched(examples)
  end

  def test_dup
    strategy = Tableflip::Strategy.new do |strategy|
      strategy.actions << :aa
      strategy.tables << :ta
      strategy.exclude_columns << :ea
      strategy.ignore_binary << :ia
      strategy.on_duplicate_max << :oa
    end

    copy = strategy.dup

    copy.actions << :ab
    copy.tables << :tb
    copy.exclude_columns << :eb
    copy.ignore_binary << :ib
    copy.on_duplicate_max << :ob

    assert_equal [ :aa ], strategy.actions
    assert_equal [ :aa, :ab ], copy.actions

    assert_equal [ :ta ], strategy.tables
    assert_equal [ :ta, :tb ], copy.tables

    assert_equal [ :ea ], strategy.exclude_columns
    assert_equal [ :ea, :eb ], copy.exclude_columns

    assert_equal [ :ia ], strategy.ignore_binary
    assert_equal [ :ia, :ib ], copy.ignore_binary

    assert_equal [ :oa ], strategy.on_duplicate_max
    assert_equal [ :oa, :ob ], copy.on_duplicate_max
  end
end

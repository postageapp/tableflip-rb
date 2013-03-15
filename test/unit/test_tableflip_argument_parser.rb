require_relative '../helper'

class TestTableflipArgumentParser < Test::Unit::TestCase
  def test_default_env
    assert_equal 'test', Tableflip::ArgumentParser.default_env('RAILS_ENV' => 'test')
  end

  def test_defaults
    strategy = Tableflip::ArgumentParser.new.parse([ ])

    assert_equal Tableflip::ArgumentParser.default_env, strategy.source_env
  end

  def test_defaults_with_env
    strategy = Tableflip::ArgumentParser.new.parse([ ], 'RAILS_ENV' => 'test')

    assert_equal 'test', strategy.source_env
  end

  def test_help
    strategy = Tableflip::ArgumentParser.new.parse(%w[ --help ])

    assert strategy.message
  end

  def test_one_table_one_action
    strategy = Tableflip::ArgumentParser.new.parse(%w[ --track example_table ])

    assert_equal [ :tracking_add ], strategy.actions
    assert_equal [ 'example_table' ], strategy.tables
  end

  def test_one_table_many_actions
    strategy = Tableflip::ArgumentParser.new.parse(%w[ --track --migrate --target=target_env example_table ])

    assert_equal [ :tracking_add, :table_migrate ], strategy.actions
    assert_equal [ 'example_table' ], strategy.tables
    assert_equal 'target_env', strategy.target_env
  end
end

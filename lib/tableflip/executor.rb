class Tableflip::Executor
  def initialize
  end

  def execute!(strategy)
    if (strategy.message)
      puts strategy.message
      exit(0)
    end

    # ...
  end
end

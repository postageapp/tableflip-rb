class Tableflip::Executor
  def initialize
  end

  def await
    @await ||= Hash.new { |h, k| h[k] = [ ] }

    Fiber.new do
      fibers = @await[Fiber.current]

      fibers << Fiber.current

      yield if (block_given?)

      fibers.delete(Fiber.current)

      if (fibers.any?)
        Fiber.yield
      end
    end.resume
  end

  def defer
    parent_fiber = Fiber.current

    fibers = @await[parent_fiber]

    fibers << Fiber.new do
      yield if (block_given?)

      fibers.delete(Fiber.current)

      if (fibers.empty?)
        EventMachine.next_tick(parent_fiber.resume)
      end
    end.resume
  end

  def execute!(strategy)
    require 'eventmachine'
    require 'em-synchrony'

    if (strategy.message)
      puts strategy.message
      exit(0)
    end

    tables = { }

    EventMachine.synchrony do
      await do
        strategy.tables.each do |table|
          defer do
            queue = strategy.actions.dup
            source_db = Tableflip::DatabaseHandle.connect(strategy.source_env)

            tables[table] = {
              :status => nil,
              :queue => queue
            }
          
            while (action = queue.shift)
              puts "#{table} [#{action}]"
              case (action)
              when :track
                add_tracking(source_db, table)
              when :remove
                remove_tracking(source_db, table)
              end
            end
          end
        end
      end

      EventMachine.stop_event_loop
    end
  end

  def add_tracking(db, table)
    changes_table = "#{table}__changes"

    if (db.table_exists?(changes_table))
      STDERR.puts("Table #{changes_table} already exists. Not recreated.")
    else
      db["CREATE TABLE `#{changes_table}` (id INT PRIMARY KEY, claim INT, INDEX index_claim (claim))"].update
    end
  end

  def remove_tracking(db, table)
    changes_table = "#{table}__changes"

    if (db.table_exists?(changes_table))
      db["DROP TABLE IF EXISTS `#{table}__changes`"].update
    else
      STDERR.puts("Table #{changes_table} does not exist. Not removed.")
    end
  end
end

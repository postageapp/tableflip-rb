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

      while (fibers.any?)
        Fiber.yield
      end
    end.resume
  end

  def defer
    parent_fiber = Fiber.current

    fibers = @await[parent_fiber]

    fiber = Fiber.new do
      yield if (block_given?)

      fibers.delete(Fiber.current)

      parent_fiber.resume
    end

    fibers << fiber

    EventMachine.next_tick do
      fiber.resume
    end
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

  def do_query(db, query)
    deferred = db.query(query)

    deferred.callback do |result|
      Fiber.resume(result)
    end

    Fiber.yield
  end

  def table_exists?(db, table)
    result = do_query(db, "SHOW FIELDS FROM `#{table}`")

    puts result.inspect
  end

  def add_tracking(db, table)
    changes_table = "#{table}__changes"

    if (table_exists?(db, changes_table))
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

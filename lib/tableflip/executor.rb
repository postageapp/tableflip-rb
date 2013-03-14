class Tableflip::Executor
  def initialize
  end

  def await
    @await ||= Hash.new { |h, k| h[k] = [ ] }

    fibers = @await[Fiber.current]

    fibers << Fiber.current

    yield if (block_given?)

    fibers.delete(Fiber.current)

    while (fibers.any?)
      Fiber.yield
    end
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
      timer = EventMachine::PeriodicTimer.new(1) do
        # puts tables.inspect
      end

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
              when :migrate
                target_db = Tableflip::DatabaseHandle.connect(strategy.target_env)
                migrate(source_db, target_db, table)
              end
            end
          end
        end
      end

      EventMachine.stop_event_loop
    end
  end

  def do_query(db, query)
    fiber = Fiber.current
    deferred = db.query(query)

    deferred.callback do |result|
      EventMachine.next_tick do
        fiber.resume(result)
      end
    end

    deferred.errback do |err|
      EventMachine.next_tick do
        fiber.resume(err)
      end
    end

    case (response = Fiber.yield)
    when Exception
      raise response
    else
      response
    end
  end

  def table_exists?(db, table)
    do_query(db, "SHOW FIELDS FROM `#{table}`")

    true

  rescue Mysql2::Error
    false
  end

  def add_tracking(db, table)
    changes_table = "#{table}__changes"

    if (table_exists?(db, changes_table))
      STDERR.puts("Table #{changes_table} already exists. Not recreated.")
    else
      do_query(db, "CREATE TABLE `#{changes_table}` (id INT PRIMARY KEY, claim INT, INDEX index_claim (claim))")
      do_query(db, "CREATE TRIGGER `#{table}__tai` AFTER INSERT ON `#{table}` FOR EACH ROW INSERT IGNORE INTO `#{changes_table}` (id) VALUES (NEW.id) ON DUPLICATE KEY UPDATE claim=NULL")
      do_query(db, "CREATE TRIGGER `#{table}__tau` AFTER UPDATE ON `#{table}` FOR EACH ROW INSERT IGNORE INTO `#{changes_table}` (id) VALUES (NEW.id) ON DUPLICATE KEY UPDATE claim=NULL")
    end
  end

  def remove_tracking(db, table)
    changes_table = "#{table}__changes"

    if (table_exists?(db, changes_table))
      do_query(db, "DROP TABLE IF EXISTS `#{table}__changes`")
      do_query(db, "DROP TRIGGER IF EXISTS `#{table}__tai`")
      do_query(db, "DROP TRIGGER IF EXISTS `#{table}__tau`")
    else
      STDERR.puts("Table #{changes_table} does not exist. Not removed.")
    end
  end

  def migrate(source_db, target_db, table)
  end
end

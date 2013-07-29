class Tableflip::Executor
  class BinaryString < String
  end

  # == Instance Methods =====================================================

  def initialize(strategy)
    @strategy = strategy

    @time_format = '%Y-%m-%d %H:%M:%S'
  end

  def log(message)
    puts "[%s] %s" % [ Time.now.strftime(@time_format), message ]
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

  def execute!
    require 'eventmachine'
    require 'em-synchrony'

    if (@strategy.message)
      puts @strategy.message
      exit(0)
    end

    tables = { }

    EventMachine.synchrony do
      if (@strategy.tables.include?(:__all__))
        source_db = Tableflip::DatabaseHandle.connect(
          @strategy.source_env,
          :encoding => @strategy.encoding
        )

        @strategy.tables.delete(:__all__)

        result = do_query(source_db, "SHOW TABLES")

        result.each do |row|
          table_name = row.first[1]

          case (table_name)
          when 'schema_migrations', /__changes/
            next
          end

          @strategy.tables << table_name
        end
      end

      await do
        @strategy.tables.each do |table|
          defer do
            queue = @strategy.actions.dup

            table_config = tables[table] = {
              :table => table,
              :queue => queue
            }
                
            while (action = queue.shift)
              log("#{table} [#{action}]")

              source_db = Tableflip::DatabaseHandle.connect(
                @strategy.source_env,
                :encoding => @strategy.encoding
              )

              case (action)
              when :tracking_add
                tracking_add(source_db, table_config)
              when :tracking_remove
                tracking_remove(source_db, table_config)
              when :tracking_seed
                tracking_seed(source_db, table_config)
              when :table_migrate
                @strategy.complete = false

                target_db = Tableflip::DatabaseHandle.connect(
                  @strategy.target_env,
                  :encoding => @strategy.encoding
                )
                table_migrate(source_db, target_db, table_config)
              when :table_report_status
                target_db = Tableflip::DatabaseHandle.connect(
                  @strategy.target_env,
                  :encoding => @strategy.encoding
                )
                table_report_status(source_db, target_db, table_config)
              when :table_count
                table_count(source_db, target_db, table_config)
              when :table_create_test
                table_create_test(source_db, table_config)
              when :table_fuzz
                table_fuzz(source_db, table_config, @strategy.fuzz_intensity)
              end
            end
          end
        end
      end

      EventMachine.stop_event_loop
    end
  end

  def escaper(db, value)
    case (value)
    when nil
      'NULL'
    when BinaryString
      "0x%s" % value.unpack("H*")
    when Fixnum
      value
    when Date
      '"' + db.escape(value.strftime('%Y-%m-%d')) + '"'
    when DateTime, Time
      '"' + db.escape(value.utc.strftime('%Y-%m-%d %H:%M:%S')) + '"'
    when Array
      value.collect { |v| escaper(db, v) }.join(',')
    else
      '"' + db.escape(value.to_s) + '"'
    end
  end

  def do_query(db, query, *values)
    fiber = Fiber.current
    query = query.gsub('?') do |s|
      escaper(db, values.shift)
    end

    if (@strategy.debug_queries?)
      puts "SQL> #{query}"
    end

    completed = false

    while (!completed)
      begin
        deferred = db.query(query)

        deferred.callback do |result|
          EventMachine.next_tick do
            completed = true

            fiber.resume(result)
          end
        end

        deferred.errback do |err|
          EventMachine.next_tick do
            completed = true

            fiber.resume(err)
          end
        end

        case (response = Fiber.yield)
        when Exception
          raise response
        else
          return response
        end

      rescue Mysql2::Error => e
        if (e.to_s.match(/MySQL server has gone away/))
          # Ignore
        else
          raise e
        end
      end
    end
  end

  def table_exists?(db, table)
    do_query(db, "SHOW FIELDS FROM `#{table}`")

    true

  rescue Mysql2::Error
    false
  end

  def tracking_add(db, table_config)
    table = table_config[:table]
    changes_table = "#{table}__changes"

    if (table_exists?(db, changes_table))
      STDERR.puts("Table #{changes_table} already exists. Not recreated.")
    else
      do_query(db, "CREATE TABLE `#{changes_table}` (id INT PRIMARY KEY, claim INT, INDEX index_claim (claim))")
      do_query(db, "CREATE TRIGGER `#{table}__tai` AFTER INSERT ON `#{table}` FOR EACH ROW INSERT IGNORE INTO `#{changes_table}` (id) VALUES (NEW.id) ON DUPLICATE KEY UPDATE claim=NULL")
      do_query(db, "CREATE TRIGGER `#{table}__tau` AFTER UPDATE ON `#{table}` FOR EACH ROW INSERT IGNORE INTO `#{changes_table}` (id) VALUES (NEW.id) ON DUPLICATE KEY UPDATE claim=NULL")
    end
  end

  def tracking_remove(db, table_config)
    table = table_config[:table]
    changes_table = "#{table}__changes"

    if (table_exists?(db, changes_table))
      do_query(db, "DROP TABLE IF EXISTS `#{table}__changes`")
      do_query(db, "DROP TRIGGER IF EXISTS `#{table}__tai`")
      do_query(db, "DROP TRIGGER IF EXISTS `#{table}__tau`")
    else
      STDERR.puts("Table #{changes_table} does not exist. Not removed.")
    end
  end

  def tracking_seed(db, table_config)
    table = table_config[:table]
    changes_table = "#{table}__changes"

    result = do_query(db, "SELECT id FROM `#{table}` #{@strategy.where}")

    ids = result.collect { |r| r[:id] }
    GC.start

    if (ids.any?)
      log("Populating #{ids.length} entries into #{changes_table} from #{table}")

      ((ids.length / @strategy.block_size) + 1).times do |n|
        start_offset = @strategy.block_size * n
        id_block = ids[start_offset, @strategy.block_size]

        if (id_block and id_block.any?)
          query = "INSERT IGNORE INTO `#{changes_table}` (id) VALUES %s" % [
            id_block.collect { |id| "(%d)" % id }.join(',')
          ]

          do_query(db, query)

          log("%d/%d entries added to #{changes_table}" % [ start_offset + id_block.length, ids.length ])
        end
      end
    else
      log("No records to migrate from #{table}")
    end
  end

  def table_report_status(source_db, target_db, table_config)
    table = table_config[:table]
    changes_table = "#{table}__changes"

    source_table_count = do_query(source_db, "SELECT COUNT(*) AS count FROM `#{table}`").first[:count]
    target_table_count = do_query(target_db, "SELECT COUNT(*) AS count FROM `#{table}`").first[:count]
    migrated_count = do_query(source_db, "SELECT COUNT(*) AS count FROM `#{changes_table}` WHERE claim IS NOT NULL").first[:count]
    tracked_count = do_query(source_db, "SELECT COUNT(*) AS count FROM `#{changes_table}`").first[:count]

    percentage = tracked_count > 0 ? (migrated_count.to_f * 100 / tracked_count) : 0.0

    log(
      "%s: %d/%d [%d/%d] (%.1f%%)" % [
        table,
        source_table_count,
        target_table_count,
        migrated_count,
        tracked_count,
        percentage
      ]
    )
  end

  def table_migrate(source_db, target_db, table_config)
    table = table_config[:table]
    changes_table = "#{table}__changes"

    result = do_query(source_db, "SELECT COUNT(*) AS rows FROM `#{changes_table}` WHERE claim IS NULL")
    count = table_config[:count] = result.first[:rows]

    log("#{table} has #{table_config[:count]} records to migrate.")

    next_claim = do_query(source_db, "SELECT MAX(claim) AS claim FROM `#{changes_table}`").first[:claim] || 0

    result = do_query(source_db, "SHOW FIELDS FROM `#{table}`")

    exclusions = Hash[
      @strategy.exclude_columns.collect do |column|
        [ column.to_sym, true ]
      end
    ]

    columns = [ ]
    binary_columns = { }

    result.each do |r|
      column = r[:Field].to_sym

      next if (exclusions[column])

      columns << column

      case (r[:Type].downcase)
      when 'tinyblob','blob','mediumblob','longblob','binary','varbinary'
        binary_columns[column] = true
      end
    end

    if (binary_columns.any?)
      log("#{table} has binary columns: #{binary_columns.keys.join(',')}")
    end

    @migrating ||= { }

    fiber = Fiber.current
    migrated = 0
    selected = 1

    loop do
      next_claim += 1
      do_query(source_db, "UPDATE `#{changes_table}` SET claim=? WHERE claim IS NULL LIMIT ?", next_claim, @strategy.block_size)

      result = do_query(source_db, "SELECT id FROM `#{changes_table}` WHERE claim=?", next_claim)

      id_block = result.to_a.collect { |r| r[:id] }

      if (id_block.length == 0)
        if (@strategy.persist?)
          EventMachine::Timer.new(1) do
            fiber.resume
          end

          Fiber.yield

          next
        else
          break
        end
      end

      log("Claim \##{next_claim} yields #{id_block.length} records.")

      selected = do_query(source_db, "SELECT * FROM `#{table}` WHERE id IN (?)", id_block)

      values = selected.collect do |row|
        "(%s)" % [
          escaper(
            source_db,
            columns.collect do |column|
              (binary_columns[column] and row[column]) ? BinaryString.new(row[column]) : row[column]
            end
          )
        ]
      end

      do_query(target_db, "REPLACE INTO `#{table}` (#{columns.collect { |c| "`#{c}`" }.join(',')}) VALUES #{values.join(',')}")

      selected = values.length
      migrated += values.length

      log("Migrated %d/%d records for #{table}" % [ migrated, count ])
    end
  end

  def table_create_test(db, table_config)
    table = table_config[:table]

    do_query(db, "CREATE TABLE `#{table}` (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), created_at DATETIME, updated_at DATETIME)")
  rescue Mysql2::Error => e
    puts e.to_s
  end

  def table_fuzz(db, table_config, count)
    require 'securerandom'

    table = table_config[:table]

    EventMachine::PeriodicTimer.new(1) do
      unless (@inserting)
        @inserting = true

        Fiber.new do
          now = Time.now.utc.strftime('%Y-%m-%d %H:%M:%S')

          log("Adding #{count} rows to #{table}")

          count.times do
            do_query(db,
              "INSERT IGNORE INTO `#{table}` (id, name, created_at, updated_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE name=VALUES(name), updated_at=VALUES(updated_at)",
              SecureRandom.random_number(1<<20),
              SecureRandom.hex,
              now,
              now
            )
          end

          @inserting = false
        end.resume
      end
    end
  end
end

require 'mysql2'
require 'yaml'

class Tableflip::DatabaseHandle
  # == Constants ============================================================
  
  DATABASE_CONFIG_FILE = 'database.yml'.freeze

  DEFAULT_OPTIONS = {
    symbolize_keys: true,
    encoding: 'utf-8'
  }.freeze

  # == Properties ===========================================================

  attr_reader :db

  # == Class Methods ========================================================

  def self.config_path
    path = Dir.pwd
    last_path = nil

    while (path != last_path)
      config_path = File.expand_path("config/#{DATABASE_CONFIG_FILE}", path)

      if (File.exist?(config_path))
        return config_path
      end

      last_path = path
      path = File.expand_path('..', path)
    end

    nil
  end

  def self.config
    @config ||= begin
      _config_path = self.config_path

      if (!_config_path)
        STDERR.puts("Could not find #{DATABASE_CONFIG_FILE}")
        exit(-1)
      elsif (File.exists?(_config_path))
        File.open(_config_path) do |f|
          YAML.load(f)
        end
      else
        STDERR.puts "Could not open #{_config_path}"
        exit(-1)
      end
    end
  end

  def self.runtime_environment
    DAEMON_ENV or 'development'
  end

  def self.environment_config(env)
    _config = self.config[env]

    unless (_config)
      raise "No environment #{env} defined in #{self.config_path}"
    end

    options = DEFAULT_OPTIONS.dup

    _config.each do |k, v|
      options[k.to_sym] = v
    end

    options[:loggers] = [ ]

    options
  end

  # == Instance Methods =====================================================

  def initialize(env, options = nil)
    @db = Sequel.connect(self.class.environment_config(env).merge(options || { }))
  end

  def method_missing(*args)
    @db.send(*args)
  end
end

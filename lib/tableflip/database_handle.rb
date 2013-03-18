require 'mysql2'
require 'mysql2/em'
require 'yaml'

class Tableflip::DatabaseHandle
  # == Constants ============================================================
  
  DATABASE_CONFIG_FILE ='database.yml'

  DEFAULT_OPTIONS = {
    :symbolize_keys => true,
    :encoding => 'UTF8'
  }.freeze

  PARAM_MAP = Hash.new do |h, k|
    k.to_sym
  end

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
      options[PARAM_MAP[k]] = v
    end

    options[:loggers] = [ ]

    options
  end

  def self.connect(env, options)
    Mysql2::EM::Client.new(self.environment_config(env).merge(options))
  end

  # == Instance Methods =====================================================

end

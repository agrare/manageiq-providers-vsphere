require "logger"

class EventCatcher
  attr_reader :hostname, :username, :password, :exit_requested
  def initialize(hostname, username, password)
    @hostname = hostname
    @username = username
    @password = password

    @exit_requested = false
  end

  def run
    vim = connect(hostname, username, password)

    until exit_requested
      sleep 1
    end

    log.info("Exiting...")
  ensure
    vim.close unless vim.nil?
  end

  def stop
    log.info("Exit request received...")
    @exit_requested = true
  end

  private

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def connect(host, username, password)
    log.info("Connecting to #{username}@#{host}...")

    vim_opts = {
      :ns       => 'urn:vim25',
      :host     => host,
      :ssl      => true,
      :insecure => true,
      :path     => '/sdk',
      :port     => 443,
      :user     => username,
      :password => password,
    }

    require 'rbvmomi/vim'

    vim = RbVmomi::VIM.connect(vim_opts)

    log.info("Connected")
    vim
  end
end

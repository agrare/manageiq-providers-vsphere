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
    until exit_requested
      vim = connect(hostname, username, password)

      monitor_events(vim)
    end

    log.info("Exiting...")
  ensure
    vim.close unless vim.nil?
  end

  def stop
    log.info("Exit request received...")
    @exit_requested = true
  end

  def monitor_events(vim)
    event_history_collector = create_event_history_collector(vim)
    property_filter = create_property_filter(vim, event_history_collector)

    version = nil
    options = RbVmomi::VIM.WaitOptions(:maxWaitSeconds => 60)

    until exit_requested
      update_set = vim.propertyCollector.WaitForUpdatesEx(:version => version, :options => options)
      next if update_set.nil?

      version = update_set.version

      log.info("Received events")
    end
  ensure
    property_filter.DestroyPropertyFilter unless property_filter.nil?
  end

  private

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def connect(host, username, password)
    log.info("Connecting to #{host}...")

    opts = {
      :ns       => "urn:vim25",
      :host     => host,
      :ssl      => true,
      :insecure => true,
      :path     => "/sdk",
      :port     => 443,
      :rev      => "6.5",
    }

    require 'rbvmomi/vim'

    conn = RbVmomi::VIM.new(opts).tap do |vim|
      vim.rev = vim.serviceContent.about.apiVersion

      log.info("Logging in to #{username}@#{host}...")
      vim.serviceContent.sessionManager.Login(
        :userName => username,
        :password => password,
      )
      log.info("Logging in to #{username}@#{host}...Complete")
    end

    log.info("Connected...")
    conn
  end

  def create_event_history_collector(vim, page_size = 20)
    filter = RbVmomi::VIM.EventFilterSpec()

    vim.serviceContent.eventManager.CreateCollectorForEvents(:filter => filter).tap do |collector|
      collector.SetCollectorPageSize(:maxCount => page_size)
    end
  end

  def create_property_filter(vim, event_history_collector)
    event_history_object_spec = RbVmomi::VIM.ObjectSpec(
      :obj => event_history_collector
    )

    event_history_prop_spec = RbVmomi::VIM.PropertySpec(
      :type    => event_history_collector.class.wsdl_name,
      :all     => false,
      :pathSet => ["latestPage"],
    )

    spec = RbVmomi::VIM.PropertyFilterSpec(
      :objectSet => [event_history_object_spec],
      :propSet   => [event_history_prop_spec],
    )

    vim.propertyCollector.CreateFilter(:spec => spec, :partialUpdates => true)
  end
end

require "logger"
require "manageiq-messaging"

require_relative "miq_queue"

class EventCatcher
  attr_reader :options, :ems_id, :exit_requested, :queue
  def initialize(options)
    @options = options
    @ems_id  = options[:ems_id]
    @queue   = MiqQueue.new(q_options)

    @exit_requested = false
    at_exit { @queue.close }
  end

  def run
    until exit_requested
      monitor_events
    end

    log.info("Exiting...")
  end

  def stop
    log.info("Exit request received...")
    @exit_requested = true
  end

  def monitor_events
    vim = connect(ems_options)

    event_history_collector = create_event_history_collector(vim)
    property_filter = create_property_filter(vim, event_history_collector)

    version = nil
    options = RbVmomi::VIM.WaitOptions(:maxWaitSeconds => 60)

    until exit_requested
      update_set = vim.propertyCollector.WaitForUpdatesEx(:version => version, :options => options)
      next if update_set.nil?

      version = update_set.version

      property_filter_update = update_set.filterSet.to_a.detect { |filter_update| filter_update.filter == property_filter }
      next if property_filter_update.nil?

      object_update_set = property_filter_update.objectSet

      object_update_set.to_a.each do |object_update|
        next if object_update.obj != event_history_collector ||
                object_update.kind != "modify"

        events = object_update.changeSet.to_a.collect do |prop_change|
          next unless prop_change.name =~ /latestPage.*/

          Array(prop_change.val).collect do |event|
            parse_event(event)
          end
        end.flatten.compact

        log.info("Received #{events.count} events")
        queue.save(events)
      end
    end
  ensure
    property_filter.DestroyPropertyFilter unless property_filter.nil?
    vim.close unless vim.nil?
  end

  private

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def connect(opts)
    host     = opts[:host]
    username = opts[:user]
    password = opts[:password]
    port     = opts[:port]

    log.info("Connecting to #{username}@#{host}...")

    opts = {
      :ns       => "urn:vim25",
      :host     => host,
      :ssl      => true,
      :insecure => true,
      :path     => "/sdk",
      :port     => port,
      :rev      => "6.5",
    }

    require 'rbvmomi'

    conn = RbVmomi::VIM.new(opts).tap do |vim|
      vim.rev = vim.serviceContent.about.apiVersion

      vim.serviceContent.sessionManager.Login(
        :userName => username,
        :password => password,
      )
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

  def parse_event(event)
    event_type = event.class.wsdl_name
    is_task = event_type == "TaskEvent"

    result = {
      :ems_id     => ems_id,
      :event_type => event_type,
      :chain_id   => event.chainId,
      :is_task    => is_task,
      :source     => "VC",
      :message    => event.fullFormattedMessage,
      :timestamp  => event.createdTime,
      :username   => event.userName,
      # TODO: full event data goes over max message size :full_data  => event,
    }

    result
  end

  def ems_options
    {
      :host     => @options[:ems_hostname],
      :port     => @options[:ems_port],
      :user     => @options[:ems_user],
      :password => @options[:ems_password],
    }
  end

  def q_options
    {
      :host     => @options[:q_hostname],
      :port     => @options[:q_port],
      :username => @options[:q_user],
      :password => @options[:q_password],
    }
  end
end

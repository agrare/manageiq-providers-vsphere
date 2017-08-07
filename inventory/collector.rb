require "logger"
require "active_support/core_ext/object/blank"

require_relative "miq_queue"
require_relative "parser"
require_relative "persister"
require_relative "collector/inventory_cache"
require_relative "collector/property_collector"

class Collector
  include InventoryCache
  include PropertyCollector

  attr_reader :ems_id, :options, :exit_requested, :queue
  def initialize(options)
    @options = options
    @ems_id  = options[:ems_id]
    @queue   = MiqQueue.new(q_options)

    @exit_requested = false
  end

  def run
    until exit_requested
      begin
        wait_for_updates
      rescue RbVmomi::Fault => err
        log.err("Caught exception #{err.message}")
      end
    end

    log.info("Exiting...")
  end

  def stop
    log.info("Exit request received...")
    @exit_requested = true
  end

  private

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def publish_inventory(inventory)
    queue.save(inventory)
  end

  def connect(opts)
    host     = opts[:host]
    username = opts[:user]
    password = opts[:password]

    log.info("Connecting to #{username}@#{host}...")

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

      vim.serviceContent.sessionManager.Login(
        :userName => username,
        :password => password,
      )
    end

    log.info("Connected...")
    conn
  end

  def wait_for_updates
    vim = connect(ems_options)

    property_filter = create_property_filter(vim)

    # Return if we don't receive any updates for 60 seconds break
    # so that we can check if we are supposed to exit
    options = RbVmomi::VIM.WaitOptions(:maxWaitSeconds => 60)

    # Send the "special initial data version" i.e. an empty string
    # so that we get all inventory back in the first update set
    version = ""

    log.info("Refreshing initial inventory...")

    initial = true
    until exit_requested
      update_set = vim.propertyCollector.WaitForUpdatesEx(:version => version, :options => options)
      next if update_set.nil?

      # Save the new update set version
      version = update_set.version

      property_filter_update_set = update_set.filterSet
      next if property_filter_update_set.blank?

      persister = Persister.new(ems_id, "ManageIQ::Providers::Vmware::InfraManager::Inventory::Persister")
      parser ||= Parser.new(persister.collections)

      property_filter_update_set.each do |property_filter_update|
        next if property_filter_update.filter != property_filter

        object_update_set = property_filter_update.objectSet
        next if object_update_set.blank?

        process_object_update_set(object_update_set) { |obj, props| parser.parse(obj, props) }
      end

      parser = nil

      inventory = persister.to_raw_data
      publish_inventory(inventory)

      next if update_set.truncated

      next unless initial

      log.info("Refreshing initial inventory...Complete")
      initial = false
    end
  ensure
    property_filter.DestroyPropertyFilter unless property_filter.nil?
    vim.close unless vim.nil?
  end

  def process_object_update_set(object_update_set, &block)
    log.info("Processing #{object_update_set.count} updates...")

    object_update_set.each do |object_update|
      process_object_update(object_update, &block)
    end

    log.info("Processing #{object_update_set.count} updates...Complete")
  end

  def process_object_update(object_update)
    managed_object = object_update.obj

    props =
      case object_update.kind
      when "enter", "modify"
        process_object_update_modify(managed_object, object_update.changeSet)
      when "leave"
        process_object_update_leave(managed_object)
      end

    yield managed_object, props if block_given?

    return managed_object, props
  end

  def process_object_update_modify(obj, change_set, _missing_set = [])
    obj_type = obj.class.wsdl_name
    obj_ref  = obj._ref

    props = inventory_cache[obj_type][obj_ref].dup

    change_set.each do |property_change|
      next if property_change.nil?

      case property_change.op
      when 'add'
        process_property_change_add(props, property_change)
      when 'remove', 'indirectRemove'
        process_property_change_remove(props, property_change)
      when 'assign'
        process_property_change_assign(props, property_change)
      end
    end

    update_inventory_cache(obj_type, obj_ref, props)

    props
  end

  def process_object_update_leave(obj)
    obj_type = obj.class.wsdl_name
    obj_ref  = obj._ref

    inventory_cache[obj_type].delete(obj_ref)

    nil
  end

  def process_property_change_add(props, property_change)
    name = property_change.name

    props[name] ||= []
    props[name] << property_change.val
  end

  def process_property_change_remove(props, property_change)
    props.delete(property_change.name)
  end

  def process_property_change_assign(props, property_change)
    props[property_change.name] = property_change.val
  end

  def ems_options
    {
      :host     => @options[:ems_hostname],
      :user     => @options[:ems_user],
      :password => @options[:ems_password],
    }
  end

  def q_options
    {
      :host     => @options[:q_hostname],
      :port     => @options[:q_port].to_i,
      :username => @options[:q_user],
      :password => @options[:q_password],
    }
  end
end

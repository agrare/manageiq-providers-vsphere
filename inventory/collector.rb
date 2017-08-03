require 'yaml'
require 'logger'
require 'rbvmomi/vim'
require 'manageiq-messaging'
require 'parser'
require 'collector/inventory_cache'
require 'collector/property_collector'
require 'inventory_collections'
require 'active_support/core_ext/object/blank'

class Collector
  include InventoryCache
  include InventoryCollections
  include PropertyCollector

  attr_reader :ems_id, :hostname, :user, :password, :exit_requested, :queue_client
  def initialize(ems_id, hostname, user, password)
    @ems_id         = ems_id
    @hostname       = hostname
    @user           = user
    @password       = password
    @exit_requested = false

    ManageIQ::Messaging.logger = Logger.new(STDOUT)
    @queue_client   = ManageIQ::Messaging::Client.open(
      :Stomp,
      :host       => "localhost",
      :port       => 61616,
      :password   => "smartvm",
      :username   => "admin",
      :client_ref => "inventory_vspere_#{ems_id}",
    )
  end

  def run
    until exit_requested
      vim = connect(hostname, user, password)

      begin
        wait_for_updates(vim)
      rescue RbVmomi::Fault => err
        log.err("Caught exception #{err.message}")
      ensure
        vim.serviceContent.sessionManager.Logout
        vim = nil
      end
    end

    log.info("Exiting...")
  ensure
    vim.serviceContent.sessionManager.Logout unless vim.nil?
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

  def wait_for_updates(vim)
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

      inventory_collections = initialize_inventory_collections
      parser ||= Parser.new(inventory_collections)

      property_filter_update_set.each do |property_filter_update|
        next if property_filter_update.filter != property_filter

        object_update_set = property_filter_update.objectSet
        next if object_update_set.blank?

        process_object_update_set(object_update_set) { |obj, props| parser.parse(obj, props) }
      end

      next if update_set.truncated

      # TODO: send inventory over artemis
      parser = nil

      next unless initial

      log.info("Refreshing initial inventory...Complete")
      initial = false
    end
  ensure
    property_filter.DestroyPropertyFilter unless property_filter.nil?
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
end

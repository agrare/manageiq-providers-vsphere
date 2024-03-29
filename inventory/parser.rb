require_relative "parser/compute_resource"
require_relative "parser/datacenter"
require_relative "parser/datastore"
require_relative "parser/folder"
require_relative "parser/host_system"
require_relative "parser/resource_pool"
require_relative "parser/virtual_machine"
require          "active_support/core_ext/string/inflections"

class Parser
  include ComputeResource
  include Datacenter
  include Datastore
  include Folder
  include HostSystem
  include ResourcePool
  include VirtualMachine

  attr_reader :collections
  def initialize(inventory_collections)
    @collections = inventory_collections
  end

  def parse(object, props)
    object_type = object.class.wsdl_name
    parse_method = "parse_#{object_type.underscore}"

    raise "Missing parser for #{object_type}" unless respond_to?(parse_method)

    send(parse_method, object, props)
  end

  def parse_compute_resource(object, props)
    collections[:ems_clusters].manager_uuids << object._ref
    return if props.nil?

    cluster_hash = {
      :ems_ref => object._ref,
      :uid_ems => object._ref,
    }

    if props.include?("name")
      cluster_hash[:name] = URI.decode(props["name"])
    end

    parse_compute_resource_summary(cluster_hash, props)
    parse_compute_resource_das_config(cluster_hash, props)
    parse_compute_resource_drs_config(cluster_hash, props)

    collections[:ems_clusters].build(cluster_hash)
  end
  alias parse_cluster_compute_resource parse_compute_resource

  def parse_datacenter(object, props)
    collections[:ems_folders].manager_uuids << object._ref
    return if props.nil?

    dc_hash = {
      :ems_ref      => object._ref,
      :uid_ems      => object._ref,
      :type         => "EmsFolder",
    }

    if props.include?("name")
      dc_hash[:name] = URI.decode(props["name"])
    end

    collections[:ems_folders].build(dc_hash)
  end

  def parse_datastore(object, props)
    collections[:storages].manager_uuids << object._ref
    return if props.nil?

    storage_hash = {
      :ems_ref => object._ref,
    }

    parse_datastore_summary(storage_hash, props)
    parse_datastore_capability(storage_hash, props)

    storage = collections[:storages].build(storage_hash)

    parse_datastore_host_mount(storage, object._ref, props)
  end

  def parse_distributed_virtual_switch(object, props)
    collections[:switches].manager_uuids << object._ref
    return if props.nil?

    switch_hash = {
      :uid_ems => object._ref,
      :shared  => true,
    }

    collections[:switches].build(switch_hash)
  end
  alias parse_vmware_distributed_virtual_switch parse_distributed_virtual_switch

  def parse_storage_pod(_object, _props)
  end

  def parse_folder(object, props)
    collections[:ems_folders].manager_uuids << object._ref
    return if props.nil?

    folder_hash = {
      :ems_ref      => object._ref,
      :uid_ems      => object._ref,
      :type         => "EmsFolder",
    }

    if props.include?("name")
      folder_hash[:name] = URI.decode(props["name"])
    end

    collections[:ems_folders].build(folder_hash)
  end

  def parse_host_system(object, props)
    collections[:hosts].manager_uuids << object._ref
    return if props.nil?

    host_hash = {
      :ems_ref => object._ref,
    }

    parse_host_system_config(host_hash, props)
    parse_host_system_product(host_hash, props)
    parse_host_system_network(host_hash, props)
    parse_host_system_runtime(host_hash, props)
    parse_host_system_system_info(host_hash, props)

    host_hash[:type] = if host_hash.include?(:vmm_product) && !%w(esx esxi).include?(host_hash[:vmm_product].to_s.downcase)
                         "ManageIQ::Providers::Vmware::InfraManager::Host"
                       else
                         "ManageIQ::Providers::Vmware::InfraManager::HostEsx"
                       end

    host = collections[:hosts].build(host_hash)

    parse_host_system_operating_system(host, props)
    parse_host_system_system_services(host, props)
    parse_host_system_hardware(host, props)
    parse_host_system_switches(host, props)
  end

  def parse_network(object, props)
  end
  alias parse_distributed_virtual_portgroup parse_network

  def parse_resource_pool(object, props)
    collections[:resource_pools].manager_uuids << object._ref
    return if props.nil?

    rp_hash = {
      :ems_ref => object._ref,
      :uid_ems => object._ref,
      :vapp    => object.kind_of?(RbVmomi::VIM::VirtualApp),
    }

    if props.include?("name")
      rp_hash[:name] = URI.decode(props["name"])
    end

    parse_resource_pool_memory_allocation(rp_hash, props)
    parse_resource_pool_cpu_allocation(rp_hash, props)

    collections[:resource_pools].build(rp_hash)
  end
  alias parse_virtual_app parse_resource_pool

  def parse_virtual_machine(object, props)
    collections[:vms_and_templates].manager_uuids << object._ref
    return if props.nil?

    vm_hash = {
      :ems_ref => object._ref,
      :vendor  => "vmware",
    }

    parse_virtual_machine_config(vm_hash, props)
    parse_virtual_machine_resource_config(vm_hash, props)
    parse_virtual_machine_summary(vm_hash, props)

    vm = collections[:vms_and_templates].build(vm_hash)

    parse_virtual_machine_operating_system(vm, props)
    parse_virtual_machine_hardware(vm, props)
    parse_virtual_machine_custom_attributes(vm, props)
    parse_virtual_machine_snapshots(vm, props)
  end
end

require "manageiq/providers/inventory"

module InventoryCollections
  def initialize_inventory_collections
    collections = {}

    [
      [:vms_and_templates,      "VmOrTemplate"],
      [:disks,                  "Disk",              :manager_ref => [:hardware, :device_name]],
      [:networks,               "Network",           :manager_ref => [:hardware, :ipaddress]],
      [:host_networks,          "Network",           :manager_ref => [:hardware, :ipaddress]],
      [:guest_devices,          "GuestDevice",       :manager_ref => [:hardware, :uid_ems]],
      [:hardwares,              "Hardware",          :manager_ref => [:vm_or_template]],
      [:host_hardwares,         "Hardware",          :manager_ref => [:host]],
      [:snapshots,              "Snapshot",          :manager_ref => [:uid]],
      [:operating_systems,      "OperatingSystem",   :manager_ref => [:vm_or_template]],
      [:host_operating_systems, "OperatingSystem",   :manager_ref => [:host]],
      [:custom_attributes,      "CustomAttribute",   :manager_ref => [:name]],
      [:ems_folders,            "EmsFolder"],
      [:resource_pools,         "ResourcePool"],
      [:ems_clusters,           "EmsCluster"],
      [:storages,               "Storage"],
      [:hosts,                  "Host"],
      [:host_storages,          "HostStorage",       :manager_ref => [:host, :storage]],
      [:host_switches,          "HostSwitch",        :manager_ref => [:host, :switch]],
      [:switches,               "Switch",            :manager_ref => [:uid_ems]],
      [:lans,                   "Lan",               :manager_ref => [:uid_ems]],
      [:storage_profiles,       "StorageProfile"],
      [:customization_specs,    "CustomizationSpec", :manager_ref => [:name]],
    ].each do |assoc, model, extra_attributes|
      attributes = {
        :model_class => model,
        :association => assoc,
      }
      attributes.merge!(extra_attributes) unless extra_attributes.nil?

      collections[assoc] = ManageIQ::Providers::Inventory::InventoryCollection.new(attributes)
    end

    collections
  end
end

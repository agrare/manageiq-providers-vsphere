class Collector
  module PropertyCollector
    EmsRefreshPropMap = {
      :VirtualMachine              => [
        "availableField",
        "config.cpuAffinity.affinitySet",
        "config.cpuHotAddEnabled",
        "config.cpuHotRemoveEnabled",
        "config.defaultPowerOps.standbyAction",
        "config.hardware.device",
        "config.hardware.numCoresPerSocket",
        "config.hotPlugMemoryIncrementSize",
        "config.hotPlugMemoryLimit",
        "config.memoryHotAddEnabled",
        "config.version",
        "datastore",
        "guest.net",
        "resourceConfig.cpuAllocation.expandableReservation",
        "resourceConfig.cpuAllocation.limit",
        "resourceConfig.cpuAllocation.reservation",
        "resourceConfig.cpuAllocation.shares.level",
        "resourceConfig.cpuAllocation.shares.shares",
        "resourceConfig.memoryAllocation.expandableReservation",
        "resourceConfig.memoryAllocation.limit",
        "resourceConfig.memoryAllocation.reservation",
        "resourceConfig.memoryAllocation.shares.level",
        "resourceConfig.memoryAllocation.shares.shares",
        "snapshot",
        "summary.vm",
        "summary.config.annotation",
        "summary.config.ftInfo.instanceUuids",
        "summary.config.guestFullName",
        "summary.config.guestId",
        "summary.config.memorySizeMB",
        "summary.config.name",
        "summary.config.numCpu",
        "summary.config.template",
        "summary.config.uuid",
        "summary.config.vmPathName",
        "summary.customValue",
        "summary.guest.hostName",
        "summary.guest.ipAddress",
        "summary.guest.toolsStatus",
        "summary.runtime.bootTime",
        "summary.runtime.connectionState",
        "summary.runtime.host",
        "summary.runtime.powerState",
        "summary.storage.unshared",
        "summary.storage.committed",
      ],
      :ComputeResource             => [
        "name",
        "host",
        "parent",
        "resourcePool",
      ],
      :ClusterComputeResource      => [
        "configuration.dasConfig.admissionControlPolicy",
        "configuration.dasConfig.admissionControlEnabled",
        "configuration.dasConfig.enabled",
        "configuration.dasConfig.failoverLevel",
        "configuration.drsConfig.defaultVmBehavior",
        "configuration.drsConfig.enabled",
        "configuration.drsConfig.vmotionRate",
        "summary.effectiveCpu",
        "summary.effectiveMemory",
        "host",
        "name",
        "parent",
        "resourcePool",
      ],
      :ResourcePool                => [
        "name",
        "parent",
        "resourcePool",
        "summary.config.cpuAllocation.expandableReservation",
        "summary.config.cpuAllocation.limit",
        "summary.config.cpuAllocation.reservation",
        "summary.config.cpuAllocation.shares.level",
        "summary.config.cpuAllocation.shares.shares",
        "summary.config.memoryAllocation.expandableReservation",
        "summary.config.memoryAllocation.limit",
        "summary.config.memoryAllocation.reservation",
        "summary.config.memoryAllocation.shares.level",
        "summary.config.memoryAllocation.shares.shares",
        "vm",
      ],
      :Folder                      => [
        "childEntity",
        "name",
        "parent",
      ],
      :Datacenter                  => [
        "datastoreFolder",
        "hostFolder",
        "name",
        "networkFolder",
        "parent",
        "vmFolder",
      ],
      :HostSystem                  => [
        "config.adminDisabled",
        "config.consoleReservation.serviceConsoleReserved",
        "config.hyperThread.active",
        "config.network.consoleVnic",
        "config.network.dnsConfig.domainName",
        "config.network.dnsConfig.hostName",
        "config.network.ipRouteConfig.defaultGateway",
        "config.network.pnic",
        "config.network.portgroup",
        "config.network.vnic",
        "config.network.vswitch",
        "config.service.service",
        "config.storageDevice.hostBusAdapter",
        "config.storageDevice.scsiLun",
        "config.storageDevice.scsiTopology.adapter",
        "datastore",
        "hardware.systemInfo.otherIdentifyingInfo",
        "name",
        "summary.host",
        "summary.config.name",
        "summary.config.product.build",
        "summary.config.product.name",
        "summary.config.product.osType",
        "summary.config.product.vendor",
        "summary.config.product.version",
        "summary.config.vmotionEnabled",
        "summary.hardware.cpuMhz",
        "summary.hardware.cpuModel",
        "summary.hardware.memorySize",
        "summary.hardware.model",
        "summary.hardware.numCpuCores",
        "summary.hardware.numCpuPkgs",
        "summary.hardware.numNics",
        "summary.hardware.vendor",
        "summary.quickStats.overallCpuUsage",
        "summary.quickStats.overallMemoryUsage",
        "summary.runtime.connectionState",
        "summary.runtime.inMaintenanceMode",
      ],
      :Datastore                   => [
        "info",
        "host",
        "capability.directoryHierarchySupported",
        "capability.perFileThinProvisioningSupported",
        "capability.rawDiskMappingsSupported",
        "summary.accessible",
        "summary.capacity",
        "summary.datastore",
        "summary.freeSpace",
        "summary.maintenanceMode",
        "summary.multipleHostAccess",
        "summary.name",
        "summary.type",
        "summary.uncommitted",
        "summary.url",
        "parent",
      ],
      :StoragePod                  => [
        "summary.capacity",
        "summary.freeSpace",
        "summary.name",
        "childEntity",
        "parent",
      ],
      :DistributedVirtualPortgroup => [
        "summary.name",
        "config.key",
        "config.distributedVirtualSwitch",
        "config.name",
        "parent",
        "host",
        "tag",
      ],
      :DistributedVirtualSwitch    => [
        "config.uplinkPortgroup",
        "config.defaultPortConfig",
        "config.numPorts",
        "summary.name",
        "summary.uuid",
        "summary.host",
        "summary.hostMember",
        "parent",
      ]
    }.freeze

    def create_property_filter(vim)
      root_folder = vim.serviceContent.rootFolder

      root_folder_object_spec = RbVmomi::VIM.ObjectSpec(
        :obj => root_folder, :selectSet => select_set
      )

      spec = RbVmomi::VIM.PropertyFilterSpec(
        :objectSet => [root_folder_object_spec],
        :propSet   => prop_set
      )

      vim.propertyCollector.CreateFilter(:spec => spec, :partialUpdates => true)
    end

    def prop_set
      EmsRefreshPropMap.collect do |type, props|
        RbVmomi::VIM.PropertySpec(
          :type    => type,
          :all     => props.nil?,
          :pathSet => props
        )
      end
    end

    def select_set
      [
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsFolder', :type => 'Folder', :path => 'childEntity',
          :selectSet => [
            RbVmomi::VIM.SelectionSpec(:name => 'tsFolder'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsDcToDsFolder'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsDcToHostFolder'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsDcToNetworkFolder'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsDcToVmFolder'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsCrToHost'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsCrToRp'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsRpToRp'),
            RbVmomi::VIM.SelectionSpec(:name => 'tsRpToVm')
          ]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsDcToDsFolder', :type => 'Datacenter', :path => 'datastoreFolder',
          :selectSet => [RbVmomi::VIM.SelectionSpec(:name => 'tsFolder')]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsDcToHostFolder', :type => 'Datacenter', :path => 'hostFolder',
          :selectSet => [RbVmomi::VIM.SelectionSpec(:name => 'tsFolder')]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsDcToNetworkFolder', :type => 'Datacenter', :path => 'networkFolder',
          :selectSet => [RbVmomi::VIM.SelectionSpec(:name => 'tsFolder')]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsDcToVmFolder', :type => 'Datacenter', :path => 'vmFolder',
          :selectSet => [RbVmomi::VIM.SelectionSpec(:name => 'tsFolder')]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsCrToHost', :type => 'ComputeResource', :path => 'host',
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsCrToRp', :type => 'ComputeResource', :path => 'resourcePool',
          :selectSet => [RbVmomi::VIM.SelectionSpec(:name => 'tsRpToRp')]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsRpToRp', :type => 'ResourcePool', :path => 'resourcePool',
          :selectSet => [RbVmomi::VIM.SelectionSpec(:name => 'tsRpToRp')]
        ),
        RbVmomi::VIM.TraversalSpec(
          :name => 'tsRpToVm', :type => 'ResourcePool', :path => 'vm',
        ),
      ]
    end
  end
end

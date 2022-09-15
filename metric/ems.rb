require "logger"
require "csv"
require "rbvmomi/vim"

class Ems
  # @option options :host       hostname
  # @option options :user       username
  # @option options :passsword  password
  attr_reader :options
  def initialize(options = {})
    @options = options
    @options[:ssl]      ||= true
    @options[:insecure] ||= true
  end

  def connection
    @connection ||= connect
  end

  def connect
    log.info("Connecting to #{@options[:host]}...")

    conn = RbVmomi::VIM.new(vim_opts).tap do |vim|
      vim.rev = vim.serviceContent.about.apiVersion
      vim.serviceContent.sessionManager.Login(
        :userName => @options[:user],
        :password => @options[:password],
      )
    end

    log.info("Connected...")
    conn
  end

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def perf_counter_info
    spec_set = [
      filter_spec(
          RbVmomi::VIM.ObjectSpec(:obj => connection.serviceContent.perfManager),
          property_spec(connection.serviceContent.perfManager.class.wsdl_name, "perfCounter")
      )
    ]
    options = RbVmomi::VIM.RetrieveOptions()

    result = connection.propertyCollector.RetrievePropertiesEx(:specSet => spec_set, :options => options)

    object_content = result&.objects&.detect { |oc| oc.obj == connection.serviceContent.perfManager }
    return if object_content.nil?

    perf_counters = object_content.propSet.to_a.detect { |prop| prop.name == "perfCounter" }
    Array(perf_counters&.val)
  end

  def perf_query(perf_counters, entities, interval: "20", start_time: nil, end_time: nil, format: "normal", max_sample: nil)
    format = RbVmomi::VIM.PerfFormat(format)

    metrics = perf_counters.map do |counter|
      RbVmomi::VIM::PerfMetricId(:counterId => counter.key, :instance  => "*")
    end

    perf_query_spec_set = entities.collect do |entity|
      RbVmomi::VIM::PerfQuerySpec(
        :entity     => entity,
        :intervalId => interval,
        :format     => format,
        :metricId   => metrics,
        :startTime  => start_time,
        :endTime    => end_time,
        :maxSample  => max_sample,
      )
    end

    entity_metrics = connection.serviceContent.perfManager.QueryPerf(:querySpec => perf_query_spec_set)

    entity_metrics
  end

  def all_powered_on_vms
    all_vms(["runtime.powerState"]).collect do |vm, props|
      power_state = props.to_a.detect { |p| p.name == "runtime.powerState" }.val.to_s
      next unless power_state == "poweredOn"
      vm
    end.compact
  end

  def capture_targets(target_options = {})
    select_set = []
    prop_set   = []
    targets    = []

    unless target_options[:exclude_vms]
      select_set.concat(vm_traversal_specs)
      prop_set << property_spec("VirtualMachine", "runtime.powerState")
    end

    unless target_options[:exclude_hosts]
      select_set.concat(host_traversal_specs)
      prop_set << property_spec("HostSystem", "runtime.connectionState")
    end

    select_set << child_traversal_specs(select_set.collect(&:name))

    object_spec = RbVmomi::VIM.ObjectSpec(
      :obj       => connection.rootFolder,
      :selectSet => select_set
    )

    result = connection.propertyCollector.RetrievePropertiesEx(
      :specSet => [filter_spec(object_spec, prop_set)], :options => RbVmomi::VIM.RetrieveOptions()
    )

    while result
      token = result.token

      result.objects.each do |object_content|
        case object_content.obj
        when RbVmomi::VIM::VirtualMachine
          vm_power_state = Array(object_content.propSet).detect { |prop| prop.name == "runtime.powerState" }
          next if vm_power_state.nil? || vm_power_state.val != "poweredOn"
        when RbVmomi::VIM::HostSystem
          host_connection_state = Array(object_content.propSet).detect { |prop| prop.name == "runtime.connectionState" }
          next if host_connection_state.nil? || host_connection_state.val != "connected"
        end

        targets << object_content.obj
      end

      break if token.nil?

      result = connection.propertyCollector.ContinueRetrievePropertiesEx(:token => token)
    end

    targets
  end

  def vm_traversal_specs
    [
      traversal_spec('tsDcToDsFolder',      'Datacenter',      'datastoreFolder', 'tsFolder'),
      traversal_spec('tsDcToNetworkFolder', 'Datacenter',      'networkFolder',   'tsFolder'),
      traversal_spec('tsDcToVmFolder',      'Datacenter',      'vmFolder',        'tsFolder'),
      traversal_spec('tsCrToRp',            'ComputeResource', 'resourcePool',    'tsRpToRp'),
      traversal_spec('tsRpToRp',            'ResourcePool',    'resourcePool',    'tsRpToRp'),
      traversal_spec('tsRpToVm',            'ResourcePool',    'vm'),
    ]
  end

  def host_traversal_specs
    [
      traversal_spec('tsDcToHostFolder',    'Datacenter',      'hostFolder',      'tsFolder'),
      traversal_spec('tsCrToHost',          'ComputeResource', 'host'),
    ]
  end

  def child_traversal_specs(selection_spec_names)
    traversal_spec('tsFolder', 'Folder', 'childEntity', selection_spec_names + ['tsFolder'])
  end

  def vm_prop_spec
    RbVmomi::VIM.PropertySpec(
      :type    => "VirtualMachine",
      :pathSet => ["runtime.powerState"],
    )
  end

  def host_prop_spec
    RbVmomi::VIM.PropertySpec(
      :type    => "HostSystem",
      :pathSet => ["runtime.connectionState"],
    )
  end

  def parse_metric(metric)
    base = {
      :mor      => metric.entity._ref,
      :children => []
    }

    samples = CSV.parse(metric['sampleInfoCSV'].to_s).first.to_a

    metric.value.to_a.collect do |value|
      id = value.id
      val = CSV.parse(value.value.to_s).first.to_a

      nh = {}.merge!(base)
      nh[:counter_id] = id.counterId
      nh[:instance]   = id.instance

      nh[:results] = []
      samples.each_slice(2).with_index do |(interval, timestamp), i|
        nh[:interval] ||= interval
        nh[:results] << timestamp
        nh[:results] << val[i].to_i
      end

      nh
    end
  end

  def close
    @connection.close if @connection
  end

  # private

  def selection_spec(names)
    Array(names).collect { |name| RbVmomi::VIM.SelectionSpec(:name => name) } if names
  end

  def traversal_spec(name, type, path, selectSet = nil)
    RbVmomi::VIM.TraversalSpec(:name => name, :type => type, :path => path, :skip => false, :selectSet => selection_spec(selectSet))
  end

  def filter_spec(object_spec, prop_set)
    RbVmomi::VIM.PropertyFilterSpec(:objectSet => Array(object_spec), :propSet => Array(prop_set))
  end

  def property_spec(type, path)
    RbVmomi::VIM.PropertySpec(:type => type, :pathSet => Array(path))
  end

  def vim_opts
    {
      :ns       => "urn:vim25",
      :host     => @options[:host],
      :ssl      => @options[:ssl],
      :insecure => @options[:insecure],
      :path     => "/sdk",
      :port     => @options[:port] || 443,
      :rev      => "6.5",
    }
  end
end

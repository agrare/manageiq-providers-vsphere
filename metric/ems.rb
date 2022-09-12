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
      RbVmomi::VIM.PropertyFilterSpec(
        :objectSet => [
          RbVmomi::VIM.ObjectSpec(
            :obj => connection.serviceContent.perfManager,
          )
        ],
        :propSet   => [
          RbVmomi::VIM.PropertySpec(
            :type    => connection.serviceContent.perfManager.class.wsdl_name,
            :pathSet => ["perfCounter"]
          )
        ],
      )
    ]
    options = RbVmomi::VIM.RetrieveOptions()

    result = connection.propertyCollector.RetrievePropertiesEx(
      :specSet => spec_set, :options => options
    )

    return if result.nil? || result.objects.nil?

    object_content = result.objects.detect { |oc| oc.obj == connection.serviceContent.perfManager }
    return if object_content.nil?

    perf_counters = object_content.propSet.to_a.detect { |prop| prop.name == "perfCounter" }
    Array(perf_counters.try(:val))
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
      prop_set << vm_prop_spec
    end

    unless target_options[:exclude_hosts]
      select_set.concat(host_traversal_specs)
      prop_set << host_prop_spec
    end

    selection_spec_names = select_set.collect { |selection_spec| selection_spec.name }
    select_set << child_entity_traversal_spec(selection_spec_names)

    object_spec = RbVmomi::VIM.ObjectSpec(
      :obj       => connection.rootFolder,
      :selectSet => select_set
    )

    filter_spec = RbVmomi::VIM.PropertyFilterSpec(
      :objectSet => [object_spec],
      :propSet   => prop_set
    )

    options = RbVmomi::VIM.RetrieveOptions()

    result = connection.propertyCollector.RetrievePropertiesEx(
      :specSet => [filter_spec], :options => options
    )

    while result
      token = result.token

      result.objects.each do |object_content|
        case object_content.obj
        when RbVmomi::VIM::VirtualMachine
          vm_props = Array(object_content.propSet)
          next if vm_props.empty?

          vm_power_state = vm_props.detect { |prop| prop.name == "runtime.powerState" }
          next if vm_power_state.nil?

          next unless vm_power_state.val == "poweredOn"
        when RbVmomi::VIM::HostSystem
          host_props = Array(object_content.propSet)
          next if host_props.empty?

          host_connection_state = host_props.detect { |prop| prop.name == "runtime.connectionState" }
          next if host_connection_state.nil?

          next unless host_connection_state.val == "connected"
        end

        targets << object_content.obj
      end

      break if token.nil?

      result = connection.propertyCollector.ContinueRetrievePropertiesEx(:token => token)
    end

    targets
  end

  def datacenter_folder_traversal_spec(path)
    RbVmomi::VIM.TraversalSpec(
      :name => "tsDatacenter#{path}",
      :type => "Datacenter",
      :path => path,
      :skip => false,
      :selectSet => [
        RbVmomi::VIM.SelectionSpec(:name => "tsFolder")
      ]
    )
  end

  def vm_traversal_specs
    [datacenter_folder_traversal_spec("vmFolder")]
  end

  def compute_resource_to_host_traversal_spec
    RbVmomi::VIM.TraversalSpec(
      :name => "tsComputeResourceToHost",
      :type => "ComputeResource",
      :path => "host",
      :skip => false,
    )
  end

  def host_traversal_specs
    [
      datacenter_folder_traversal_spec("hostFolder"),
      compute_resource_to_host_traversal_spec
    ]
  end

  def child_entity_traversal_spec(selection_spec_names = [])
    select_set = selection_spec_names.map do |name|
      RbVmomi::VIM.SelectionSpec(:name => name)
    end

    RbVmomi::VIM.TraversalSpec(
      :name => 'tsFolder',
      :type => 'Folder',
      :path => 'childEntity',
      :skip => false,
      :selectSet => select_set,
    )
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

  def vim_opts
    {
      :ns       => "urn:vim25",
      :host     => @options[:host],
      :ssl      => @options[:ssl],
      :insecure => @options[:insecure],
      :path     => "/sdk",
      :port     => 443,
      :rev      => "6.5",
    }
  end
end

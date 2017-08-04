require "logger"
require "csv"
require "rbvmomi/vim"

class MetricsCollector
  attr_reader :hostname, :username, :password, :collect_interval, :exit_requested
  def initialize(hostname, username, password, collect_interval = 60)
    @hostname = hostname
    @username = username
    @password = password

    @collect_interval = collect_interval
    @exit_requested = false
  end

  def run
    conn = connect(hostname, username, password)

    # At startup request the list of all counters from the PerformanceManager
    perf_counters_by_name = {}
    perf_counter_info(conn).to_a.each do |counter|
      perf_counters_by_name[perf_counter_key(counter)] = counter
    end

    perf_counters_to_collect = METRIC_CAPTURE_COUNTERS.collect do |counter_name|
      perf_counters_by_name[counter_name]
    end

    start_time = nil

    until exit_requested
      vms = get_all_powered_on_vms(conn)
      entity_metrics = perf_query(conn, perf_counters_to_collect, vms, format: "csv", start_time: start_time)
      start_time = Time.now

      sleep(collect_interval)
    end

    log.info("Exiting...")
  ensure
    conn.close unless conn.nil?
  end

  def stop
    log.info("Exit requested...")
    @exit_requested = true
  end

  private

  METRIC_CAPTURE_COUNTERS = [
    :cpu_usage_rate_average,
    :cpu_usagemhz_rate_average,
    :mem_usage_absolute_average,
    :disk_usage_rate_average,
    :net_usage_rate_average,
    :sys_uptime_absolute_latest,
    :cpu_ready_delta_summation,
    :cpu_system_delta_summation,
    :cpu_wait_delta_summation,
    :cpu_used_delta_summation,
    :mem_vmmemctl_absolute_average,
    :mem_vmmemctltarget_absolute_average,
    :mem_swapin_absolute_average,
    :mem_swapout_absolute_average,
    :mem_swapped_absolute_average,
    :mem_swaptarget_absolute_average,
    :disk_devicelatency_absolute_average,
    :disk_kernellatency_absolute_average,
    :disk_queuelatency_absolute_average
  ].freeze

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

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def perf_counter_key(counter)
    group  = counter.groupInfo.key.downcase
    name   = counter.nameInfo.key.downcase
    rollup = counter.rollupType.downcase
    stats  = counter.statsType.downcase

    "#{group}_#{name}_#{stats}_#{rollup}".to_sym
  end

  def perf_counter_info(vim)
    log.info("Retrieving perf counters...")

    spec_set = [
      RbVmomi::VIM.PropertyFilterSpec(
        :objectSet => [
          RbVmomi::VIM.ObjectSpec(
            :obj => vim.serviceContent.perfManager,
          )
        ],
        :propSet   => [
          RbVmomi::VIM.PropertySpec(
            :type    => vim.serviceContent.perfManager.class.wsdl_name,
            :pathSet => ["perfCounter"]
          )
        ],
      )
    ]
    options = RbVmomi::VIM.RetrieveOptions()

    result = vim.propertyCollector.RetrievePropertiesEx(
      :specSet => spec_set, :options => options
    )

    return if result.nil? || result.objects.nil?

    object_content = result.objects.detect { |oc| oc.obj == vim.serviceContent.perfManager }
    return if object_content.nil?

    perf_counters = object_content.propSet.to_a.detect { |prop| prop.name == "perfCounter" }.val

    log.info("Retrieving perf counters...Complete")
    perf_counters
  end

  def perf_query(vim, perf_counters, entities, interval: "20", start_time: nil, end_time: nil, format: "normal", max_sample: nil)
    log.info("Collecting performance counters...")

    format = RbVmomi::VIM.PerfFormat(format)

    metrics = perf_counters.map do |counter|
      RbVmomi::VIM::PerfMetricId(
        :counterId => counter.key,
        :instance  => ""
      )
    end

    all_metrics = []
    entity_metrics = entities.each_slice(250) do |entity_set|
      perf_query_spec_set = entity_set.collect do |entity|
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

      log.info("Querying perf for #{entity_set.count} VMs...")
      entity_metrics = vim.serviceContent.perfManager.QueryPerf(:querySpec => perf_query_spec_set)
      log.info("Querying perf for #{entity_set.count} VMs...Complete")

      all_metrics.concat(entity_metrics)
    end

    log.info("Collecting performance counters...Complete")

    all_metrics
  end

  def get_all_powered_on_vms(conn)
    get_all_vms(conn, ["runtime.powerState"]).collect do |vm, props|
      power_state = props.to_a.detect { |p| p.name == "runtime.powerState" }.val.to_s
      next unless power_state == "poweredOn"
      vm
    end.compact
  end

  def get_all_vms(conn, path_set = [])
    filter_spec = RbVmomi::VIM.PropertyFilterSpec(
      :objectSet => [
        :obj => conn.rootFolder,
        :selectSet => [
          RbVmomi::VIM.TraversalSpec(
            :name => 'tsFolder',
            :type => 'Folder',
            :path => 'childEntity',
            :skip => false,
            :selectSet => [
              RbVmomi::VIM.SelectionSpec(:name => 'tsFolder'),
              RbVmomi::VIM.SelectionSpec(:name => 'tsDatacenterVmFolder'),
            ]
          ),
          RbVmomi::VIM.TraversalSpec(
            :name => 'tsDatacenterVmFolder',
            :type => 'Datacenter',
            :path => 'vmFolder',
            :skip => false,
            :selectSet => [
              RbVmomi::VIM.SelectionSpec(:name => 'tsFolder')
            ]
          )
        ],
      ],
      :propSet => [
        RbVmomi::VIM.PropertySpec(
          :type => "VirtualMachine",
          :pathSet => path_set,
        )
      ]
    )

    result = conn.propertyCollector.RetrieveProperties(:specSet => [filter_spec])
    result.to_a.collect { |r| [r.obj, r.propSet] }
  end
end

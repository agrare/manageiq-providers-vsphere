require "rbvmomi/vim"
require_relative "metrics_collector"

metric_capture_counters = [
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

def connect
  RbVmomi::VIM.connect(
    :host => ENV["EMS_HOSTNAME"],
    :user => ENV["EMS_USERNAME"],
    :password => ENV["EMS_PASSWORD"],
    :ssl => true,
    :insecure => true,
  )
end

def all_vms(conn)
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
    :propSet => [RbVmomi::VIM.PropertySpec(
      :type => "VirtualMachine",
      :pathSet => ["runtime.powerState"]
    )]
  )

  result = conn.propertyCollector.RetrieveProperties(:specSet => [filter_spec])
  result.to_a.collect do |r|
    r.obj
  end.compact
end

conn = connect

vms = all_vms(conn)

puts "Retrieving metrics for #{vms.count}"

collector = MetricsCollector.new(conn)

perf_counters_by_name = {}
collector.perf_counter_info.to_a.each do |counter|
  group  = counter.groupInfo.key.downcase
  name   = counter.nameInfo.key.downcase
  rollup = counter.rollupType.downcase
  stats  = counter.statsType.downcase

  perf_counter_name = "#{group}_#{name}_#{stats}_#{rollup}".to_sym
  perf_counters_by_name[perf_counter_name] = counter
end

interval = "20" # Realtime

counters_to_collect = metric_capture_counters.map do |counter_name|
  perf_counters_by_name[counter_name]
end

metrics = collector.perf_query(counters_to_collect, interval, vms)

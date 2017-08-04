require "logger"
require "csv"
require "rbvmomi/vim"

require_relative 'ems'

class MetricsCollector
  attr_reader :collect_interval, :exit_requested, :options
  attr_reader :ems
  def initialize(options)
    @options  = options

    @ems = Ems.new(ems_options)

    @collect_interval = options[:collect_interval] || 60
    @exit_requested = false
  end

  def run
    conn = ems.connection

    perf_counters_to_collect = ems.counters_to_collect(METRIC_CAPTURE_COUNTERS)

    start_time = nil

    until exit_requested
      vms = ems.all_powered_on_vms
      entity_metrics = ems.perf_query(perf_counters_to_collect, vms, format: "csv", start_time: start_time)
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

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def ems_options
    {
      :host     => @options[:ems_hostname],
      :user     => @options[:ems_user],
      :password => @options[:ems_password],
    }
  end
end

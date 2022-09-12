require "logger"
require "csv"
require "rbvmomi"
require "active_support/core_ext/numeric/time"

require_relative 'ems'
require_relative 'miq_queue'

class MetricsCollector
  attr_reader :collect_interval, :exit_requested, :query_size, :options
  attr_accessor :format, :interval, :interval_name, :ems_id, :initial_start_time
  attr_reader :ems, :miq_queue
  def initialize(options)
    @options  = options

    @ems       = Ems.new(ems_options)
    @miq_queue = MiqQueue.new(q_options)

    @collect_interval   = options[:collect_interval] || 60
    @query_size         = options[:perf_query_size] || 250
    @format             = options[:format] || "csv"
    @initial_start_time = options[:initial_start_time] || Time.now - 5.minutes
    @interval           = options[:interval] || "20"
    @interval_name      = capture_interval_to_interval_name(interval)
    @ems_id             = options[:ems_id]
    @exit_requested     = false
  end

  def run
    log.info("Retrieving perf counters...")
    all_perf_counters = ems.perf_counter_info
    log.info("Retrieving perf counters...Complete - Count: [#{all_perf_counters.size}]")

    perf_counters_to_collect = counters_to_collect(METRIC_CAPTURE_COUNTERS, all_perf_counters)

    perf_counters_by_id = {}
    perf_counters_to_collect.each do |counter|
      perf_counters_by_id[counter.key] = counter
    end

    start_time = initial_start_time
    end_time = nil

    until exit_requested
      log.info("Collecting performance counters...")

      perf_query_options = {
        :interval   => interval,
        :format     => format,
        :start_time => start_time,
        :end_time   => end_time
      }

      log.info("Retrieving Targets...")
      targets = ems.capture_targets(target_options)
      log.info("Retrieving Targets...Complete - Count [#{targets.count}]")

      log.info("Collecting metrics...")

      entity_metrics = []
      targets.each_slice(query_size) do |vms|
        entity_metrics.concat(ems.perf_query(perf_counters_to_collect, vms, perf_query_options))
      end
      log.info("Collecting metrics...Complete")

      log.info("Parsing metrics...")
      metrics_payload = entity_metrics.collect do |metric|
        counters       = {}
        counter_values = Hash.new { |h, k| h[k] = {} }

        processed_res = ems.parse_metric(metric)
        processed_res.each do |res|
          full_vim_key = "#{res[:counter_id]}_#{res[:instance]}"

          counter_info = perf_counters_by_id[res[:counter_id]]

          counters[full_vim_key] = {
            :counter_key           => perf_counter_key(counter_info),
            :rollup                => counter_info.rollupType,
            :precision             => counter_info.unitInfo.key == "percent" ? 0.1 : 1,
            :unit_key              => counter_info.unitInfo.key,
            :vim_key               => res[:counter_id].to_s,
            :instance              => res[:instance],
            :capture_interval      => res[:interval],
            :capture_interval_name => capture_interval_to_interval_name(res[:interval]),
          }

          Array(res[:results]).each_slice(2) do |timestamp, value|
            counter_values[timestamp][full_vim_key] = value
          end
        end

        {
          :ems_id         => ems_id,
          :ems_ref        => metric.entity._ref,
          :ems_klass      => vim_entity_to_miq_model(metric.entity),
          :interval_name  => interval_name,
          :start_range    => start_time,
          :end_range      => end_time,
          :counters       => counters,
          :counter_values => counter_values
        }
      end
      log.info("Parsing metrics...Complete")

      log.info("Sending metrics...")
      miq_queue.save(metrics_payload)
      log.info("Sending metrics...Complete")

      log.info("Collecting performance counters...Complete")

      start_time = Time.now

      sleep(collect_interval)
    end

    log.info("Exiting...")
  ensure
    ems.close
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

  def capture_interval_to_interval_name(interval)
    case interval
    when "20"
      "realtime"
    else
      "hourly"
    end
  end

  def vim_entity_to_miq_model(entity)
    case entity.class.wsdl_name
    when "VirtualMachine"
      "Vm"
    when "HostSystem"
      "Host"
    when "ClusterComputeResource"
      "EmsCluster"
    when "Datastore"
      "Storage"
    when "ResourcePool"
      "ResourcePool"
    end
  end

  def perf_counter_key(counter)
    group  = counter.groupInfo.key.downcase
    name   = counter.nameInfo.key.downcase
    rollup = counter.rollupType.downcase
    stats  = counter.statsType.downcase

    "#{group}_#{name}_#{stats}_#{rollup}".to_sym
  end

  def perf_counters_by_name(all_perf_counters)
    all_perf_counters.to_a.each_with_object({}) do |counter, hash|
      hash[perf_counter_key(counter)] = counter
    end
  end

  def counters_to_collect(perf_counter_names, all_perf_counters)
    hash = perf_counters_by_name(all_perf_counters)
    perf_counter_names.map do |counter_name|
      hash[counter_name]
    end
  end

  def ems_options
    {
      :ems_id   => @options[:ems_id],
      :host     => @options[:ems_hostname],
      :user     => @options[:ems_user],
      :password => @options[:ems_password],
      :ssl      => @options[:ems_ssl],
      :insecure => @options[:ems_insecure],
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

  def target_options
    {
      :exclude_hosts => @options[:exclude_hosts],
      :exclude_vms   => @options[:exclude_vms],
    }
  end
end

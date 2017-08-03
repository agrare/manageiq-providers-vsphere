require "logger"
require "csv"

class MetricsCollector
  attr_reader :vim
  def initialize(vim)
    @vim = vim
  end

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def perf_counter_info
    log.info("Retrieving perf counters...")

    performance_manager = vim.serviceContent.perfManager
    spec_set = [
      RbVmomi::VIM.PropertyFilterSpec(
        :objectSet => [
          RbVmomi::VIM.ObjectSpec(
            :obj => performance_manager,
          )
        ],
        :propSet   => [
          RbVmomi::VIM.PropertySpec(
            :type    => performance_manager.class.wsdl_name,
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

    object_content = result.objects.detect { |oc| oc.obj == performance_manager }
    return if object_content.nil?

    perf_counters = object_content.propSet.to_a.detect { |prop| prop.name == "perfCounter" }.val

    log.info("Retrieving perf counters...Complete")
    perf_counters
  end

  def perf_query(perf_counters, interval, entities)
    perf_manager = vim.serviceContent.perfManager

    log.info("Collecting performance counters...")

    metrics = perf_counters.map do |counter|
      RbVmomi::VIM::PerfMetricId(
        :counterId => counter.key,
        :instance  => ""
      )
    end

    all_metrics = []
    entity_metrics = entities.each_slice(250).collect do |entity_set|
      perf_query_spec_set = entity_set.collect do |entity|
        RbVmomi::VIM::PerfQuerySpec(
          :entity     => entity,
          :intervalId => interval,
          :format     => RbVmomi::VIM::PerfFormat("csv"),
          :metricId   => metrics,
          :startTime  => Time.now - 5 * 60
        )
      end

      log.info("Querying perf for #{entity_set.count} VMs...")
      entity_metrics = perf_manager.QueryPerf(:querySpec => perf_query_spec_set)
      log.info("Querying perf for #{entity_set.count} VMs...Complete")

      entity_metrics.each do |entity_metric|
        sample_info = CSV.parse(entity_metric.sampleInfoCSV)
        entity_metric.value.map do |value|
          metric_value = CSV.parse(value.value)
          all_metrics << [sample_info.first, metric_value]
        end
      end
    end

    log.info("Collecting performance counters...Complete")

    all_metrics
  end
end

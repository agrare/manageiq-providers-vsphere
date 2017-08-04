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

  def perf_query(perf_counters, entities, interval: "20", start_time: nil, end_time: nil, format: "normal", max_sample: nil)
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
end

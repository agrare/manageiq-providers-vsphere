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

    metrics = []
    perf_counters.each do |counter|
      metrics << RbVmomi::VIM::PerfMetricId(
        :counterId => counter.key,
        :instance  => ""
      )
      metrics << RbVmomi::VIM::PerfMetricId(
        :counterId => counter.key,
        :instance  => "*"
      )
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

  def all_vms(path_set = [])
    filter_spec = RbVmomi::VIM.PropertyFilterSpec(
      :objectSet => [
        :obj => connection.rootFolder,
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

    result = connection.propertyCollector.RetrieveProperties(:specSet => [filter_spec])
    result.to_a.collect { |r| [r.obj, r.propSet] }
  end

  def parse_metric(metric)
    base = {
      :mor      => metric.entity._ref,
      :children => []
    }

    samples = CSV.parse(metric.sampleInfoCSV.to_s).first.to_a

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

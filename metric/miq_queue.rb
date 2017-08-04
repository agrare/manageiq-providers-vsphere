class MiqQueue
  # @option options :host       hostname
  # @option options :port       port number (typically 61616)
  # @option options :username   username
  # @option options :passsword  password
  # @option options :client_ref descriptor on client connection (default: collector)
  def initialize(options)
    @options = options
    options[:client_ref] ||= "metrics_collector"
  end

  def save(metrics)
    # puts metrics.inspect
    puts metrics.count
    # client.publish_message(
    #   :service  => 'inventory',
    ##  :affinity => 'ems_vmware1',
    #   :message  => 'save_metrics',
    #   :payload  => metrics
    # )
  end


  def connection
    @connection ||= ManageIQ::Messaging::Client.open(@options)
  end

  def close
    @connection && @connection.close
  end
end

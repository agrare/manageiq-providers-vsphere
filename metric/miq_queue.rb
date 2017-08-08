require "manageiq-messaging"

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
    connection.publish_message(
      :service  => 'metrics',
      :message  => 'save_metrics',
      :payload  => metrics
    )
  end

  def connection
    @connection ||= ManageIQ::Messaging::Client.open(@options)
  end

  def close
    @connection && @connection.close
  end
end

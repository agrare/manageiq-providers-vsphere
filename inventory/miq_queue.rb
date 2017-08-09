require "manageiq-messaging"

class MiqQueue
  # @option options :host       hostname
  # @option options :port       port number (typically 61616)
  # @option options :username   username
  # @option options :passsword  password
  # @option options :client_ref descriptor on client connection (default: collector)
  def initialize(options)
    @options = options
    @options[:client_ref] ||= "inventory_collector"
    @options[:timeout]    ||= 60

    ManageIQ::Messaging.logger = Logger.new(STDOUT) if @options[:debug]
  end

  def save(inventory)
    connection.publish_message(
      :service => 'inventory',
      :message => 'save_inventory',
      :payload => inventory,
    )
  end


  def connection
    @connection ||= ManageIQ::Messaging::Client.open(@options)
  end

  def close
    @connection && @connection.close
  end
end

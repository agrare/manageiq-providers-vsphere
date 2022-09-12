require "optimist"
require "metrics_collector"

STDOUT.sync = true
Thread.abort_on_exception = true

def main(args)
  ManageIQ::Messaging.logger = Logger.new(STDOUT) if args[:debug]

  collector = MetricsCollector.new(args)

  thread = Thread.new { collector.run }

  begin
    loop { sleep 1 }
  rescue Interrupt
    collector.stop
    thread.run if thread.status == "sleep"
    thread.join
  end
end

def parse_args
  args = Optimist.options do
    opt :ems_id,       "ems id",       :type => :integer, :default => ENV["EMS_ID"]&.to_i
    opt :ems_hostname, "ems hostname", :type => :string,  :default => ENV["EMS_HOSTNAME"]
    opt :ems_user,     "ems username", :type => :string,  :default => ENV["EMS_USERNAME"]
    opt :ems_password, "ems password", :type => :string
    opt :ems_ssl,      "ems ssl",      :type => :boolean, :default => true
    opt :ems_port,     "ems port",     :type => :integer, :default => ENV["EMS_PORT"]&.to_i || 443

    opt :q_hostname, "queue hostname", :type => :string,  :default => ENV["QUEUE_HOSTNAME"] || "localhost" 
    opt :q_port,     "queue port",     :type => :integer, :default => ENV["QUEUE_PORT"]&.to_i || 61616
    opt :q_user,     "queue username", :type => :string,  :default => ENV["QUEUE_USER"] || "admin"
    opt :q_password, "queue password", :type => :string

    opt :exclude_hosts, "exclude hosts", :type => :boolean
    opt :exclude_vms,   "exclude vms",   :type => :boolean

    opt :debug,      "debug",          :type => :flag
    opt :timeout,    "queue timeout",  :type => :integer
    opt :heartbeat,  "queue heartbeat (true, false, value)",  :type => :string
  end

  args[:ems_password] ||= ENV["EMS_PASSWORD"]
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"
  if args[:heartbeat] == "true"
    args[:heartbeat] = true
  elsif args[:heartbeat] == "false"
    args[:heartbeat] = false
  elsif args[:heartbeat]
    args[:heartbeat] = args[:heartbeat].to_i
  end

  %i(ems_id ems_hostname ems_user ems_password).each do |param|
    raise Optimist::CommandlineError, "--#{param} required" if args[param].nil?
  end

  args
end

args = parse_args

main args

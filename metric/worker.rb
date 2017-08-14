require "trollop"
require_relative "metrics_collector"

Thread.abort_on_exception = true

def main(args)
  ManageIQ::Messaging.logger = Logger.new(STDOUT) if args[:debug]

  collector = MetricsCollector.new(args)

  thread = Thread.new { collector.run }

  begin
    loop { sleep 1 }
  rescue Interrupt
    collector.stop
    thread.join
  end
end

def parse_args
  args = Trollop.options do
    opt :ems_id,       "ems id",       :type => :int
    opt :ems_hostname, "ems hostname", :type => :string
    opt :ems_user,     "ems username", :type => :string
    opt :ems_password, "ems password", :type => :string

    opt :q_hostname, "queue hostname", :type => :string
    opt :q_port,     "queue port",     :type => :integer
    opt :q_user,     "queue username", :type => :string
    opt :q_password, "queue password", :type => :string

    opt :exclude_hosts, "exclude hosts", :type => :boolean
    opt :exclude_vms,   "exclude vms",   :type => :boolean

    opt :debug,      "debug",          :type => :flag
    opt :timeout,    "queue timeout",  :type => :integer
    opt :heartbeat,  "queue heartbeat (true, false, value)",  :type => :string
  end

  args[:ems_id]       ||= ENV["EMS_ID"]
  args[:ems_hostname] ||= ENV["EMS_HOSTNAME"]
  args[:ems_user]     ||= ENV["EMS_USERNAME"]
  args[:ems_password] ||= ENV["EMS_PASSWORD"]

  args[:q_hostname]   ||= ENV["QUEUE_HOSTNAME"] || "localhost"
  args[:q_port]       ||= ENV["QUEUE_PORT"]     || "61616"
  args[:q_user]       ||= ENV["QUEUE_USER"]     || "admin"
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"

  args[:q_port] = args[:q_port].to_i
  if args[:heartbeat] == "true"
    args[:heartbeat] = true
  elsif args[:heartbeat] == "false"
    args[:heartbeat] = false
  elsif args[:heartbeat]
    args[:heartbeat] = args[:heartbeat].to_i
  end

  %i(ems_id ems_hostname ems_user ems_password).each do |param|
    raise Trollop::CommandlineError, "--#{param} required" if args[param].nil?
  end

  args
end

args = parse_args

main args

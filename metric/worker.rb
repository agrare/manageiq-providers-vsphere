require "trollop"
require_relative "metrics_collector"

def main args
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
    opt :ems_hostname, "ems hostname", :type => :string
    opt :ems_user,     "ems username", :type => :string
    opt :ems_password, "ems password", :type => :string

    opt :q_hostname, "queue hostname", :type => :string
    opt :q_port,     "queue port",     :type => :integer
    opt :q_user,     "queue username", :type => :string
    opt :q_password, "queue password", :type => :string
  end

  args[:ems_hostname] ||= ENV["EMS_HOSTNAME"]
  args[:ems_user]     ||= ENV["EMS_USERNAME"]
  args[:ems_password] ||= ENV["EMS_PASSWORD"]

  args[:q_hostname]   ||= ENV["QUEUE_HOSTNAME"] || "localhost"
  args[:q_port]       ||= ENV["QUEUE_PORT"]     || "61616"
  args[:q_user]       ||= ENV["QUEUE_USER"]     || "admin"
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"

  args[:q_port] = args[:q_port].to_i

  # %i(ems_hostname ems_user ems_password q_hostname q_port q_user q_password).each do |param|
  #   raise Trollop::CommandlineError, "--#{param} required" if args[param].nil?
  # end

  args
end

args = parse_args

main args

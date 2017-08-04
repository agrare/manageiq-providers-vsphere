require "trollop"
require_relative "metrics_collector"

def main args
  collector = MetricsCollector.new(args[:hostname], args[:user], args[:password])

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
    opt :hostname, "hostname", :type => :string
    opt :user,     "username", :type => :string
    opt :password, "password", :type => :string
  end

  args[:hostname] ||= ENV["EMS_HOSTNAME"]
  args[:user]     ||= ENV["EMS_USERNAME"]
  args[:password] ||= ENV["EMS_PASSWORD"]

  raise Trollop::CommandlineError, "--hostname required" if args[:hostname].nil?
  raise Trollop::CommandlineError, "--user required"     if args[:user].nil?
  raise Trollop::CommandlineError, "--password required" if args[:password].nil?

  args
end

args = parse_args

main args

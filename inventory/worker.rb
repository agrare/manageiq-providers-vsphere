require "optimist"
require_relative "collector"

STDOUT.sync = true

def main args
  collector = Collector.new(args)
  log = Logger.new(STDOUT)

  thread = Thread.new do
    begin
      collector.run
    rescue => err
      log.error(err.message)
      log.error(err.backtrace.join("\n"))
    end
  end

  begin
    loop { break unless thread.alive?; sleep 1 }
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
  end

  args[:ems_password] ||= ENV["EMS_PASSWORD"]
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"

  %i(ems_id ems_hostname ems_user ems_password q_hostname q_port q_user q_password).each do |param|
    raise Optimist::CommandlineError, "--#{param} required" if args[param].nil?
  end

  args
end

args = parse_args

main args

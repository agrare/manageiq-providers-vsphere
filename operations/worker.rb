require "trollop"
require_relative "provider_operations"

args = Trollop.options do
  opt :hostname, "hostname", :type => :string
  opt :username, "username", :type => :string
  opt :password, "password", :type => :string
end

ENV["PROVIDER_HOSTNAME"] ||= args[:hostname]
ENV["PROVIDER_USERNAME"] ||= args[:username]
ENV["PROVIDER_PASSWORD"] ||= args[:password]

raise Trollop::CommandlineError, "--hostname required" if ENV["PROVIDER_HOSTNAME"].nil?
raise Trollop::CommandlineError, "--username required" if ENV["PROVIDER_USERNAME"].nil?
raise Trollop::CommandlineError, "--password required" if ENV["PROVIDER_PASSWORD"].nil?

ProviderOperations.run!

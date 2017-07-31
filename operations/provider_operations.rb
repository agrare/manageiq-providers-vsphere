require "sinatra/base"
require "connection_pool"

class ProviderOperations < Sinatra::Application
  attr_reader :hostname, :username, :password

  def initialize
    @hostname = ENV["PROVIDER_HOSTNAME"]
    @username = ENV["PROVIDER_USERNAME"]
    @password = ENV["PROVIDER_PASSWORD"]

    $vim = ConnectionPool.new(size: 3, timeout: 30) { vim_connect }
    super
  end

  get "/vms/:ref" do
    with_provider_vm { |vm| vm.collect "name" }
  end

  post "/vms/:ref/start" do
    with_provider_vm { |vm| vm.PowerOnVM_Task }
  end

  post "/vms/:ref/stop" do
    with_provider_vm { |vm| vm.ShutdownGuest }
  end

  private

  def with_provider_connection
    $vim.with { |vim| yield vim }
  end

  def with_provider_vm
    with_provider_connection do |vim|
      vm = RbVmomi::VIM::VirtualMachine.new(vim, params[:ref])
      yield vm
    end
  end

  def vim_connect
    require "rbvmomi/vim"

    RbVmomi::VIM.connect(
      host:     hostname, 
      user:     username,
      password: password,
      ssl:      true,
      insecure: true
    )
  end
end

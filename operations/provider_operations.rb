require "sinatra/base"
require "connection_pool"

class ProviderOperations < Sinatra::Application
  attr_reader :hostname, :username, :password

  def initialize
    @hostname = ENV["PROVIDER_HOSTNAME"]
    @username = ENV["PROVIDER_USERNAME"]
    @password = ENV["PROVIDER_PASSWORD"]

    $vim = ConnectionPool.new(size: 2, timeout: 30) { vim_connect }

    super
  end

  get "/vms/:ref" do
    with_provider_vm { |vm| vm.collect "name" }
  end

  post "/vms/:ref/start" do
    with_provider_vm { |vm| vm.PowerOnVM_Task }
  end

  post "/vms/:ref/stop" do
    with_provider_vm { |vm| vm.PowerOffVM_Task }
  end

  post "/vms/:ref/reset" do
    with_provider_vm { |vm| vm.ResetVM_Task }
  end

  post "/vms/:ref/shutdown_guest" do
    with_provider_vm { |vm| vm.ShutdownGuest }
  end

  post "/vms/:ref/reboot_guest" do
    with_provider_vm { |vm| vm.RebootGuest }
  end

  post "/vms/:ref/unregister" do
    with_provider_vm { |vm| vm.UnregisterVM }
  end

  post "/vms/:ref/mark_as_template" do
    with_provider_vm { |vm| vm.MarkAsTemplate }
  end

  post "/vms/:ref/mark_as_vm" do
    with_provider_vm do |vm|
      connection = vm._connection

      pool = RbVmomi::VIM::ResourcePool(connection, params[:pool])
      host = RbVmomi::VIM::Host(connection, params[:host]) if params[:host]

      vm.MarkAsVirtualMachine(pool: pool, host: host)
    end
  end

  post "/vms/:ref/migrate" do
    with_provider_vm do |vm|
      connection = vm._connection

      pool     = RbVmomi::VIM::ResourcePool(connection, params[:pool])  if params[:pool]
      host     = RbVmomi::VIM::HostSystem(connection, params[:host])    if params[:host]
      priority = RbVmomi::VIM::VirtualMachineMovePriority(params[:priority])
      state    = RbVmomi::VIM::VirtualMachinePowerState(params[:state]) if params[:state]

      vm.MigrateVM_Task(
        pool:     pool,
        host:     host,
        priority: priority,
        state:    state
      )
    end
  end

  post "/vms/:ref/relocate" do
  end

  post "/vms/:ref/clone" do
  end

  # TODO: vm_connect_all
  # TODO: vm_disconnect_all
  # TODO: vm_connect_cdrom
  # TODO: vm_disconnect_cdrom
  # TODO: vm_connect_floppy
  # TODO: vm_disconnect_floppy
  # TODO: vm_connect_disconnect_cdrom
  # TODO: vm_connect_disconnect_floppy
  # TODO: vm_connect_disconnect_all_connectable_devices
  # TODO: vm_connect_disconnect_specified_connectable_devices

  post "/vms/:ref/create_snapshot" do
  end

  post "/vms/:ref/remove_snapshot" do
  end

  post "/vms/:ref/remove_all_snapshots" do
  end

  post "/vms/:ref/revert_to_snapshot" do
  end

  post "/vms/:ref/rename" do
    with_provider_vm do |vm|
      name = params[:name]
      vm.Rename_Task(newName: name)
    end
  end

  private

  def log
    @logger ||= Logger.new(STDOUT)
  end

  def with_provider_connection
    $vim.with { |vim| yield vim }
  end

  def with_provider_vm
    with_provider_connection do |vim|
      yield RbVmomi::VIM::VirtualMachine(vim, params[:ref])
    end
  end

  def vim_connect
    log.info("Connecting to #{username}@#{hostname}...")

    require "rbvmomi/vim"
    vim = RbVmomi::VIM.connect(
      host:     hostname, 
      user:     username,
      password: password,
      ssl:      true,
      insecure: true
    )

    log.info("Connecting to #{username}@#{hostname}...Complete")

    vim
  end
end

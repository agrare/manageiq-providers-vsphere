class Collector
  module Connection
    def connect hostname, user, password
      vim_opts = {
        :ns       => 'urn:vim25',
        :rev      => '4.1',
        :host     => hostname,
        :ssl      => true,
        :insecure => true,
        :path     => '/sdk',
        :port     => 443,
      }

      RbVmomi::VIM.new(vim_opts).tap do |vim|
        vim.rev = vim.serviceContent.about.apiVersion
        vim.serviceContent.sessionManager.Login(:userName => user, :password => password)
      end
    end
  end
end

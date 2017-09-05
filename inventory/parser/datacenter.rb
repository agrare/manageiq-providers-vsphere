class Parser
  module Datacenter
    def parse_datacenter_children(dc_hash, props)
      dc_hash[:ems_children] = {:folder => []}

      if props.include?("datastoreFolder")
        dc_hash[:ems_children][:folder] << collections[:ems_folders].find_or_build(props["datastoreFolder"]._ref)
      end
      if props.include?("hostFolder")
        dc_hash[:ems_children][:folder] << collections[:ems_folders].find_or_build(props["hostFolder"]._ref)
      end
      if props.include?("networkFolder")
        dc_hash[:ems_children][:folder] << collections[:ems_folders].find_or_build(props["networkFolder"]._ref)
      end
      if props.include?("vmFolder")
        dc_hash[:ems_children][:folder] << collections[:ems_folders].find_or_build(props["vmFolder"]._ref)
      end
    end
  end
end

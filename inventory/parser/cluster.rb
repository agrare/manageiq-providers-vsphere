class Parser
  module Cluster
    def parse_cluster_summary(props)
      result = {}

      if props.include? "summary.effectiveCpu"
        result[:effective_cpu]    = props["summary.effectiveCpu"].to_i
      end
      if props.include? "summary.effectiveMemory"
        result[:effective_memory] = props["summary.effectiveMemory"].to_i * 1024 * 1024
      end

      result
    end

    def parse_cluster_das_config(props)
      result = {}

      result[:ha_enabled]       = props["configuration.dasConfig.enabled"].to_s.downcase == "true" if props.include?("configuration.dasConfig.enabled")
      result[:ha_admit_control] = props["configuration.dasConfig.admissionControlEnabled"]         if props.include?("configuration.dasConfig.admissionControlEnabled")
      result[:ha_max_failures]  = props["configuration.dasConfig.failoverLevel"]                   if props.include?("configuration.dasConfig.failoverLevel")

      result
    end

    def parse_cluster_drs_config(props)
      result = {}

      result[:drs_enabled]             = props["configuration.drsConfig.enabled"].to_s.downcase == "true" if props.include?("configuration.drsConfig.enabled")
      result[:drs_automation_level]    = props["configuration.drsConfig.defaultVmBehavior"]               if props.include?("configuration.drsConfig.defaultVmBehavior")
      result[:drs_migration_threshold] = props["configuration.drsConfig.vmotionRate"]                     if props.include?("configuration.drsConfig.vmotionRate")

      result
    end
  end
end

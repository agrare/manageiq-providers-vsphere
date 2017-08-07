require "yaml"
require_relative "inventory_collections"

class Persister
  include InventoryCollections

  attr_reader :collections, :ems_id, :type
  def initialize(ems_id, type)
    @collections = initialize_inventory_collections
    @ems_id      = ems_id
    @type        = type
  end

  def to_raw_data
    collections_data = collections.map do |key, collection|
      next if collection.data.blank? && collection.manager_uuids.blank? && collection.all_manager_uuids.nil?

      {
        :name              => key,
        :manager_uuids     => collection.manager_uuids,
        :all_manager_uuids => collection.all_manager_uuids,
        :data              => collection.to_raw_data
      }
    end.compact

    {
      :ems_id => ems_id,
      :class  => type,
      :collections => collections_data
    }
  end
end

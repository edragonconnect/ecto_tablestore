defmodule EctoTablestore.Repo.Migrations.Create do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_search_index_test") do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:name, :string)
    end

    create search_index("migration_search_index_test", "migration_search_index_test_index") do
      field_schema_keyword("name")
      field_schema_keyword("content")
      field_schema_integer("inserted_at")
      field_schema_integer("updated_at")
      field_schema_boolean("is_published")
      field_sort("name")
    end
  end
end

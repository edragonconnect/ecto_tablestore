defmodule EctoTablestore.Repo.Migrations.Name1 do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_test1") do
      add_pk :id, :integer, partition_key: true
      add_pk :name, :string
      add_column :col1, :integer
      add_column :col2, :double
      add_column :col3, :boolean
      add_column :col4, :string
      add_column :col5, :binary
    end

    create secondary_index("migration_test1", "migration_test1_index") do
      add_pk :col1
      add_pk :id
      add_column :col2
    end
  end
end

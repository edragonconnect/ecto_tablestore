defmodule EctoTablestore.Repo.Migrations.Name2 do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_test2") do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:name, :string)
    end
  end
end

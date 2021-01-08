defmodule EctoTablestore.Repo.Migrations.Name1 do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_test1") do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:name, :string)
    end
  end
end

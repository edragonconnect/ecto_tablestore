defmodule EctoTablestore.Repo.Migrations.Name1 do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_test1") do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:name, :string)
      add_attr(:attr1, :integer)
      add_attr(:attr2, :double)
      add_attr(:attr3, :boolean)
      add_attr(:attr4, :string)
      add_attr(:attr5, :binary)
    end

    create secondary_index("migration_test1", "migration_test1_index", [:attr1, :id], [:attr2])
  end
end

defmodule EctoTablestore.Repo.Migrations.Name2 do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_test2") do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_attr(:attr1, :integer)
      add_attr(:attr2, :double)
      add_attr(:attr3, :boolean)
      add_attr(:attr4, :string)
      add_attr(:attr5, :binary)
      add_index("migration_test2_index1", [:attr1, :id], [:attr2])
      add_index("migration_test2_index2", [:attr4, :id], [:attr1, :attr2, :attr3, :attr5])
    end
  end
end

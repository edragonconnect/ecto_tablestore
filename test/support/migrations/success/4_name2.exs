defmodule EctoTablestore.Repo.Migrations.Name2 do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    create table("migration_test2") do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_column(:col1, :integer)
      add_column(:col2, :double)
      add_column(:col3, :boolean)
      add_column(:col4, :string)
      add_column(:col5, :binary)
      add_index("migration_test2_index1", [:col1, :id], [:col2])
      add_index("migration_test2_index2", [:col4, :id], [:col1, :col2, :col3, :col5])
    end
  end
end

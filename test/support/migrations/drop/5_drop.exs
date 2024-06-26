defmodule EctoTablestore.Repo.Migrations.Drop do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    drop table("migration_test1")
    drop_if_exists table("migration_test1")

    drop secondary_index("migration_test2", "migration_test2_index1")
    drop secondary_index("migration_test2", "migration_test2_index2")
    drop_if_exists secondary_index("migration_test2", "migration_test2_index2")
    drop_if_exists secondary_index("migration_test2", "unknown_table_index_xx01")
    drop_if_exists secondary_index("unknown_table_xx01", "migration_test2_index2")
    drop_if_exists secondary_index("unknown_table_xx01", "unknown_table_index_xx01")
  end
end

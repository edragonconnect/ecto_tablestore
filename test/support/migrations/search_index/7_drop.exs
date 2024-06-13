defmodule EctoTablestore.Repo.Migrations.DropSearchIndex do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    drop search_index("migration_search_index_test", "migration_search_index_test_index")
    drop_if_exists search_index("migration_search_index_test", "migration_search_index_test_index")
    drop_if_exists search_index("migration_search_index_test", "unknown_search_index_xx01")
    drop_if_exists search_index("unknown_table_xx01", "migration_search_index_test_index")
    drop_if_exists search_index("unknown_table_xx01", "unknown_search_index_xx01")
    drop table("migration_search_index_test")
  end
end

defmodule EctoTablestore.Repo.Migrations.Drop do
  @moduledoc false
  use EctoTablestore.Migration

  def change do
    drop(search_index("migration_search_index_test", "migration_search_index_test_index"))
    drop(table("migration_search_index_test"))
  end
end

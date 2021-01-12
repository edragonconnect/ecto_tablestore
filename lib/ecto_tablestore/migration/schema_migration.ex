defmodule EctoTablestore.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  # The table name defaults to `schema_migrations`.
  @moduledoc false
  use EctoTablestore.Schema
  require Logger
  import EctoTablestore.Query
  alias Ecto.{Changeset, MigrationError}

  tablestore_schema "schema_migrations" do
    field(:version, :integer, primary_key: true)
    timestamps(updated_at: false)
  end

  def ensure_schema_migrations_table!(repo) do
    table_name =
      if prefix = Keyword.get(repo.config(), :migration_default_prefix) do
        prefix <> "schema_migrations"
      else
        "schema_migrations"
      end

    repo_meta = Ecto.Adapter.lookup_meta(repo)
    instance = repo_meta.instance
    primary_keys = [{"version", :integer}]

    case ExAliyunOts.create_table(instance, table_name, primary_keys) do
      :ok ->
        result_str = IO.ANSI.format([:green, "ok", :reset])
        table_name_str = IO.ANSI.format([:green, table_name, :reset])

        Logger.info(fn ->
          ">>>> create migrations table: #{table_name_str} result: #{result_str}"
        end)

        :ok

      {:error, %{code: "OTSObjectAlreadyExist"}} ->
        :ok

      result ->
        raise MigrationError,
              "create migrations table: #{table_name} result: #{inspect(result)}"
    end
  end

  def versions(repo) do
    repo.stream_range(__MODULE__, [version: :inf_min], [version: :inf_max],
      columns_to_get: ["version"]
    )
    |> Enum.map(& &1.version)
    |> Enum.sort()
  end

  def up(repo, version) do
    repo.insert(%__MODULE__{version: version}, condition: condition(:expect_not_exist))
  end

  def delete(repo, version) do
    repo.delete(%__MODULE__{version: version}, condition: condition(:ignore))
  end

  def lock_version!(repo, version, fun) do
    %__MODULE__{version: version}
    |> Changeset.change()
    |> Changeset.check_constraint(:condition,
      name: "OTSConditionCheckFail",
      message: "already_exist"
    )
    |> repo.insert(condition: condition(:expect_not_exist))
    |> case do
      {:ok, _} ->
        try do
          case fun.() do
            {:error, error} -> raise error
            result -> result
          end
        rescue
          error ->
            delete(repo, version)
            raise error
        end

      {:error, %Changeset{errors: [condition: {"already_exist", _}]}} ->
        raise MigrationError,
              "lock_version failed because of the version: #{version} already have"

      {:error, error} ->
        raise error
    end
  end
end

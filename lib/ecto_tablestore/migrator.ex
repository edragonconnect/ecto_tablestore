defmodule EctoTablestore.Migrator do
  @moduledoc false
  require Logger
  alias EctoTablestore.Migration.{Runner, SchemaMigration}

  def run(repo, migration_source, opts) do
    repo_meta = Ecto.Adapter.lookup_meta(repo.get_dynamic_repo())
    instance = repo_meta.instance
    table_names = initialize_table_names(instance)

    SchemaMigration.ensure_schema_migrations_table!(repo, table_names)
    versions = SchemaMigration.versions(repo)

    pending =
      migration_source
      |> migrations_for()
      |> Enum.filter(fn {version, _name, _file} -> not (version in versions) end)

    ensure_no_duplication!(pending)

    versions =
      pending
      |> Enum.map(&load_migration!/1)
      |> Enum.map(fn {version, module} ->
        SchemaMigration.lock_version!(repo, version, fn ->
          attempt(repo, version, module, :change, opts) ||
            {:error,
             Ecto.MigrationError.exception(
               "#{inspect(module)} does not implement a `change/0` function"
             )}
        end)

        version
      end)

    if match?([], versions) do
      Logger.info("Already done")
    end

    destroy_table_names(instance)
    versions
  end

  def with_repo(repo, fun, opts \\ []) do
    ensure_table_names_ets()
    config = repo.config()

    mode = Keyword.get(opts, :mode, :permanent)
    apps = [:ecto_tablestore | config[:start_apps_before_migration] || []]

    extra_started =
      Enum.flat_map(apps, fn app ->
        {:ok, started} = Application.ensure_all_started(app, mode)
        started
      end)

    {:ok, repo_started} = repo.__adapter__.ensure_all_started(config, mode)
    started = extra_started ++ repo_started
    pool_size = Keyword.get(opts, :pool_size, 2)

    case repo.start_link(pool_size: pool_size) do
      {:ok, _} ->
        try do
          {:ok, fun.(repo), started}
        after
          repo.stop()
        end

      {:error, {:already_started, _pid}} ->
        try do
          {:ok, fun.(repo), started}
        after
          if Process.whereis(repo) do
            %{pid: pid} = Ecto.Adapter.lookup_meta(repo)
            Supervisor.restart_child(repo, pid)
          end
        end

      {:error, _} = error ->
        error
    end
  end

  @table_names_ets :ecto_tablestore_table_names

  def ensure_table_names_ets() do
    if :ets.info(@table_names_ets, :size) == :undefined do
      :ets.new(@table_names_ets, [:bag, :named_table, :public, read_concurrency: true])
    end
  end

  def initialize_table_names(instance) do
    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(instance)
    objects = Enum.map(table_names, &{instance, &1})
    :ets.insert(@table_names_ets, objects)
    table_names
  end

  def destroy_table_names(instance) do
    :ets.delete(@table_names_ets, instance)
  end

  def list_table_names(instance) do
    Enum.map(:ets.lookup(@table_names_ets, instance), &elem(&1, 1))
  end

  def add_table(instance, table_name) do
    :ets.insert(@table_names_ets, {instance, table_name})
  end

  # This function will match directories passed into `Migrator.run`.
  defp migrations_for(migration_source) when is_binary(migration_source) do
    Path.join([migration_source, "**", "*.exs"])
    |> Path.wildcard()
    |> Enum.map(&extract_migration_info/1)
    |> Enum.filter(& &1)
    |> Enum.sort()
  end

  defp extract_migration_info(file) do
    base = Path.basename(file)

    case Integer.parse(Path.rootname(base)) do
      {integer, "_" <> name} -> {integer, name, file}
      _ -> nil
    end
  end

  defp ensure_no_duplication!([{version, name, _file} | t]) do
    cond do
      List.keyfind(t, version, 0) ->
        raise Ecto.MigrationError,
              "migrations can't be executed, migration version #{version} is duplicated"

      List.keyfind(t, name, 1) ->
        raise Ecto.MigrationError,
              "migrations can't be executed, migration name #{name} is duplicated"

      true ->
        ensure_no_duplication!(t)
    end
  end

  defp ensure_no_duplication!([]), do: :ok

  defp load_migration!({version, _, mod}) when is_atom(mod) do
    if migration?(mod) do
      {version, mod}
    else
      raise Ecto.MigrationError, "module #{inspect(mod)} is not an EctoTablestore.Migration"
    end
  end

  defp load_migration!({version, _, file}) when is_binary(file) do
    loaded_modules = file |> Code.compile_file() |> Enum.map(&elem(&1, 0))

    if mod = Enum.find(loaded_modules, &migration?/1) do
      {version, mod}
    else
      raise Ecto.MigrationError,
            "file #{Path.relative_to_cwd(file)} does not define an EctoTablestore.Migration"
    end
  end

  defp migration?(mod) do
    function_exported?(mod, :__migration__, 0)
  end

  defp attempt(repo, version, module, operation, opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, operation, 0) do
      Runner.run(repo, version, module, operation, opts)
      :ok
    end
  end
end

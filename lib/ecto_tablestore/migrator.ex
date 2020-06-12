defmodule EctoTablestore.Migrator do
  @moduledoc false

  alias EctoTablestore.Migration.Runner

  def run(repo, migration_source, opts) do
    migration_source
    |> migrations_for()
    |> Enum.map(&load_migration!/1)
    |> Enum.map(fn {version, migration_module} ->
      attempt(repo, version, migration_module, :change, opts)
    end)
  end

  def with_repo(repo, fun, opts \\ []) do
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

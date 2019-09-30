defmodule EctoTablestore.Migration.Runner do
  @moduledoc false
  use Agent, restart: :temporary

  alias EctoTablestore.Migration.Table
  
  require Logger

  def run(repo, version, module, operation, opts) do
    level = Keyword.get(opts, :log, :info)
    log = %{level: level}
    args = {self(), repo, module, log}

    {:ok, runner} = DynamicSupervisor.start_child(EctoTablestore.MigratorSupervisor, {__MODULE__, args})
    metadata(runner, opts)

    log(level, "== Running #{version} #{inspect module}.#{operation}/0")
    {time, _} = :timer.tc(fn -> perform_operation(module, operation) end)
    log(level, "== Migrated #{version} in #{inspect(div(time, 100_000) / 10)}s")

    stop()
  end

  def start_link({parent, repo, module, log}) do
    Agent.start_link(fn ->
      Process.link(parent)

      %{
        repo: repo,
        migration: module,
        command: nil,
        subcommands: [],
        commands: [],
        log: log,
        config: repo.config()
      }
    end)
  end

  def end_command do
    Agent.update runner(), fn state ->
      {operation, object} = state.command
      command = {operation, object, Enum.reverse(state.subcommands)}
      %{state | command: nil, subcommands: [], commands: [command | state.commands]}
    end
  end

  def subcommand(subcommand) do
    reply =
      Agent.get_and_update(runner(), fn
        %{command: nil} = state ->
          {:error, state}
        state ->
          {:ok, update_in(state.subcommands, &[subcommand|&1])}
      end)

    case reply do
      :ok ->
        :ok
      :error ->
        raise Ecto.MigrationError, message: "cannot execute command outside of block"
    end
  end

  def execute(command) do
    reply =
      Agent.get_and_update(runner(), fn
        %{command: nil} = state ->
          {:ok, %{state | subcommands: [], commands: [command | state.commands]}}
        %{command: _} = state ->
          {:error, %{state | command: nil}}
      end)

    case reply do
      :ok ->
        :ok
      :error ->
        raise Ecto.MigrationError, "cannot execute nested commands"
    end
  end

  def start_command(command) do
    reply =
      Agent.get_and_update(runner(), fn
        %{command: nil} = state ->
          {:ok, %{state | command: command}}
        %{command: _} = state ->
          {:error, %{state | command: command}}
      end)

    case reply do
      :ok ->
        :ok
      :error ->
        raise Ecto.MigrationError, "cannot execute nested commands"
    end
  end

  def repo do
    Agent.get(runner(), & &1.repo)
  end

  def prefix do
    case Process.get(:ecto_tablestore_migration) do
      %{prefix: prefix} -> prefix
      _ -> "could not find migration runner process for #{inspect self()}"
    end
  end

  def repo_config(key, default) do
    Agent.get(runner(), &Keyword.get(&1.config, key, default))
  end

  defp perform_operation(module, operation) do
    apply(module, operation, [])

    flush()
  end

  defp stop() do
    Agent.stop(runner())
  end

  defp metadata(runner, opts) do
    prefix = opts[:prefix]
    Process.put(:ecto_tablestore_migration, %{runner: runner, prefix: prefix && to_string(prefix)})
  end

  defp log(false, _msg), do: :ok
  defp log(level, msg), do: Logger.log(level, msg)

  defp runner do
    case Process.get(:ecto_tablestore_migration) do
      %{runner: runner} -> runner
      _ -> raise "could not find migration runner process for #{inspect self()}"
    end
  end

  defp flush do
    %{commands: commands, repo: repo, log: _level, migration: _migration} =
      Agent.get_and_update(runner(), fn state -> {state, %{state | commands: []}} end)

    for command <- commands do
      do_execute(repo, command)
    end
  end

  defp do_execute(repo, {:create, %Table{} = _table, columns} = command) when length(columns) <= 4 do
    command = verify_command_to_table(command)
    repo.__adapter__.execute_ddl(repo, command)
  end
  defp do_execute(_repo, {:create, _table, columns}) do
    raise Ecto.MigrationError, message: "can only have up to 4 primary keys, but get #{length(columns)} primary keys: #{inspect columns}"
  end

  defp verify_command_to_table({:create, %Table{partition_key: true} = _table, columns} = command) when length(columns) == 1 do
    command
  end
  defp verify_command_to_table({:create, %Table{partition_key: true} = table, columns} = command) when length(columns) > 1 do
    [_auto_generated_id_col, {_, _field_name, _field_type, opts} | _] = columns
    # Use defined partition key to instead of the auto generated.
    if Keyword.get(opts, :partition_key, false) do
      {:create, table, List.delete_at(columns, 0)}
    else
      command
    end
  end
  defp verify_command_to_table({:create, %Table{partition_key: false} = _table, _columns} = command) do
    command
  end

end

defmodule EctoTablestore.Migration.Runner do
  @moduledoc false
  use Agent, restart: :temporary
  require Logger

  def run(repo, version, module, operation, opts) do
    level = Keyword.get(opts, :log, :info)
    args = {self(), repo, module, %{level: level}}

    {:ok, runner} =
      DynamicSupervisor.start_child(EctoTablestore.MigratorSupervisor, {__MODULE__, args})

    Process.put(:ecto_tablestore_runner, runner)

    log(level, "== Running #{version} #{inspect(module)}.#{operation}/0")

    {time, _} = :timer.tc(fn -> perform_operation(module, operation) end)
    time = System.convert_time_unit(time, :microsecond, :millisecond) / 1000

    log(level, "== Migrated #{version} in #{time}s")

    Agent.stop(runner)
  end

  def start_link({parent, repo, module, log}) do
    Agent.start_link(fn ->
      Process.link(parent)

      %{
        repo: repo,
        migration: module,
        commands: [],
        log: log,
        config: repo.config()
      }
    end)
  end

  def repo do
    Agent.get(runner(), & &1.repo)
  end

  def repo_config(key, default) do
    Agent.get(runner(), &Keyword.get(&1.config, key, default))
  end

  defp perform_operation(module, operation) do
    apply(module, operation, [])

    flush()
  end

  defp log(false, _msg), do: :ok
  defp log(level, msg), do: Logger.log(level, msg)

  defp runner do
    case Process.get(:ecto_tablestore_runner) do
      nil -> raise "could not find migration runner process for #{inspect(self())}"
      runner -> runner
    end
  end

  def push_command(fun) when is_function(fun, 1) do
    Agent.update(runner(), &%{&1 | commands: [fun | &1.commands]})
  end

  defp flush do
    %{commands: commands, repo: repo} = Agent.get(runner(), & &1)

    commands
    |> Enum.reverse()
    |> Enum.each(& &1.(repo))
  end
end

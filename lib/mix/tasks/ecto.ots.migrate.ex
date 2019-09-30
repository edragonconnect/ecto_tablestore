defmodule Mix.Tasks.Ecto.Ots.Migrate do
  use Mix.Task

  import Mix.Ecto
  import Mix.EctoTablestore

  @shortdoc "Run the table creations"

  @aliases [
    n: :step,
    r: :repo
  ]

  @switches [
    all: :boolean,
    step: :integer,
    to: :integer,
    prefix: :string,
    repo: [:keep, :string]
  ]

  @impl true
  def run(args) do

    repos = parse_repo(args)

    {opts, _} = OptionParser.parse! args, strict: @switches, aliases: @aliases

    opts = if opts[:to] || opts[:step] || opts[:all], do: opts, else: Keyword.put(opts, :all, true)

    {:ok, _} = Application.ensure_all_started(:ecto_tablestore)

    for repo <- repos do
      ensure_repo(repo, args)
      path = ensure_migrations_path(repo, opts)

      migrator = &EctoTablestore.Migrator.run/3
      fun = &migrator.(&1, path, opts)

      case EctoTablestore.Migrator.with_repo(repo, fun, [mode: :temporary] ++ opts) do
        {:ok, _migrated, _apps} -> :ok
        {:error, error} -> Mix.raise "Could not start repo #{inspect repo}, error: #{inspect error}"
      end
    end

  end

end

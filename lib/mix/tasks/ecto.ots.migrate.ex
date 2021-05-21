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
    repo: [:keep, :string],
    migrations_path: :string
  ]

  @moduledoc """
  Runs the all migration for the given repository.

  Migrations are expected at "priv/tablestore/YOUR_REPO/migrations" directory of the current
  application, where "YOUR_REPO" is the last segment in your repository name. For example, the
  repository `MyApp.Repo` will use "priv/tablestore/repo/migrations", the repository
  `Whatever.MyRepo` will use "priv/tablestore/my_repo/migrations".

  Alibaba Tablestore is a cloud service product, there will not do any local storage, meanwhile it
  is a NoSQL data storage, the migrations mainly responsible for the creation of the table primary
  key(s).

  Currently, run migrations will execute the all defined tasks, once Tablestore's primary key(s)
  are created, the primary keys(s) cannot be modified, failed to create an existing table, we can
  ignore it.

  ## Example

      mix ecto.ots.migrate
      mix ecto.ots.migrate -r EctoTablestore.TestRepo

  ## Command line options

    * `-r`, `--repo` - the repo to migrate
  """

  @impl true
  def run(args) do
    repos = parse_repo(args)

    {opts, _} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

    opts =
      if opts[:to] || opts[:step] || opts[:all], do: opts, else: Keyword.put(opts, :all, true)

    {:ok, _} = Application.ensure_all_started(:ecto_tablestore)

    for repo <- repos do
      ensure_repo(repo, args)
      path = ensure_migrations_path(repo, opts)
      fun = &EctoTablestore.Migrator.run(&1, path, opts)

      case EctoTablestore.Migrator.with_repo(repo, fun, [mode: :temporary] ++ opts) do
        {:ok, _migrated, _apps} ->
          :ok

        {:error, error} ->
          Mix.raise("Could not start repo #{inspect(repo)}, error: #{inspect(error)}")
      end
    end
  end
end

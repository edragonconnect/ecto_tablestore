defmodule Mix.Tasks.Ecto.Ots.Gen.Migration do
  use Mix.Task

  import Macro, only: [camelize: 1, underscore: 1]
  import Mix.Generator
  import Mix.Ecto
  import Mix.EctoTablestore

  @shortdoc "Generate a migration to create tablestore table"

  @aliases [
    r: :repo
  ]

  @switches [
    change: :string,
    repo: [:string, :keep]
  ]

  @moduledoc """
  """

  @doc false
  def run(args) do

    no_umbrella!("ecto.ots.gen.migration")

    repos = parse_repo(args)

    Enum.map repos, fn repo ->
      case OptionParser.parse!(args, strict: @switches, aliases: @aliases) do
        {opts, [name]} ->
          ensure_repo(repo, args)

          path = Path.join(source_repo_priv(repo), "migrations")

          base_name = "#{underscore(name)}.exs"

          file = Path.join(path, "#{timestamp()}_#{base_name}")
          unless File.dir?(path), do: create_directory path

          fuzzy_path = Path.join(path, "*_#{base_name}")
          if Path.wildcard(fuzzy_path) != [] do
            Mix.raise "migration can't be created, there is already a migration file with name #{name}."
          end

          assigns = [mod: Module.concat([repo, Migrations, camelize(name)]), change: opts[:change]]

          create_file file, migration_template(assigns)

          if open?(file) and Mix.shell.yes?("Do you want to run this migration?") do
            Mix.Task.run "ecto.migrate", ["-r", inspect(repo)]
          end

          file

        {_, _} ->
          Mix.raise "expected ecto.ots.gen.migration to receive the migration file name, " <>
                    "got: #{inspect Enum.join(args, " ")}"
      end
    end
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: << ?0, ?0 + i >>
  defp pad(i), do: to_string(i)

  embed_template :migration, """
  defmodule <%= inspect @mod %> do
    use EctoTablestore.Migration

    def change do
  <%= @change %>
    end
  end
  """
end

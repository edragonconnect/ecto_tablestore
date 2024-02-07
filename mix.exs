defmodule EctoTablestore.MixProject do
  use Mix.Project

  @source_url "https://github.com/edragonconnect/ecto_tablestore"

  def project do
    [
      app: :ecto_tablestore,
      version: "0.15.0",
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {EctoTablestore.Application, []}
    ]
  end

  defp deps do
    [
      {:ecto, "~> 3.11"},
      {:ex_aliyun_ots, github: "xinz/ex_aliyun_ots"},
      {:jason, "~> 1.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:hashids, "~> 2.0", optional: true}
    ]
  end

  defp description do
    "Alibaba Tablestore adapter for Ecto"
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README.md", "LICENSE.md", ".formatter.exs"],
      maintainers: ["Kevin Pan", "Xin Zou"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      formatter_opts: [gfm: true],
      extras: [
        "README.md",
        "CHANGELOG.md"
      ],
      groups_for_docs: [
        group_for_function("Query API"),
        group_for_function("Schema API"),
        group_for_function("Transaction API"),
        group_for_function("Runtime API")
      ],
      groups_for_modules: [
        Types: [
          Ecto.Hashids,
          Ecto.ReplaceableString
        ],
        Migration: [
          EctoTablestore.Migration
        ]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp group_for_function(group), do: {String.to_atom(group), &(&1[:group] == group)}
end

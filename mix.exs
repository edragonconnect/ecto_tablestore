defmodule EctoTablestore.MixProject do
  use Mix.Project

  @source_url "https://github.com/edragonconnect/ecto_tablestore"

  def project do
    [
      app: :ecto_tablestore,
      version: "0.10.1",
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
      {:ecto, "~> 3.2"},
      {:ex_aliyun_ots, "~> 0.11"},
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
      files: ["lib", "mix.exs", "README.md", "LICENSE.md"],
      maintainers: ["Xin Zou"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      formatter_opts: [gfm: true],
      extras: [
        "README.md"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end

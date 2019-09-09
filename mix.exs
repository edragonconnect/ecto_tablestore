defmodule EctoTablestore.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_tablestore,
      version: "0.1.0",
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {EctoTablestore.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.1"},
      # {:ex_aliyun_ots, path: "../ex_aliyun_ots"},
      {:ex_aliyun_ots, github: "xinz/ex_aliyun_ots", branch: "0.4"},
      {:ex_doc, "~> 0.20", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end

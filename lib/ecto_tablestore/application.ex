defmodule EctoTablestore.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: EctoTablestore.MigratorSupervisor}
    ]

    opts = [strategy: :one_for_one, name: EctoTablestore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

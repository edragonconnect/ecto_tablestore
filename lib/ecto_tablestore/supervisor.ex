defmodule Ecto.Adapters.Tablestore.Supervisor do
  @moduledoc false
  use DynamicSupervisor

  def start_link([]) do
    DynamicSupervisor.start_link(__MODULE__, [])
  end

  def init([]) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

end

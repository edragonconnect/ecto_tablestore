defmodule Ecto.Adapters.Tablestore.Supervisor do
  @moduledoc false
  use DynamicSupervisor

  def start_link([name: name]) do
    DynamicSupervisor.start_link(__MODULE__, [], name: name)
  end

  def init([]) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

end

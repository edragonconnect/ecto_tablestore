defmodule TestHelper do
  alias EctoTablestore.Repo

  def setup_all() do
    Repo.start_link()
    :ok
  end
end

ExUnit.start(seed: 0)

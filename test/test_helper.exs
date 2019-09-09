defmodule TestHelper do
  alias EctoTablestore.TestRepo

  def setup_all() do
    TestRepo.start_link()
    :ok
  end
end

ExUnit.start(seed: 0)

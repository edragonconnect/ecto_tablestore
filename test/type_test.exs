defmodule EctoTablestore.TypeTest do
  use ExUnit.Case

  import Ecto.Type

  alias EctoTablestore.Integer

  test "custom internal type" do
    assert load(Integer, 1) == {:ok, 1}
    assert dump(Integer, 1) == {:ok, 1}
    assert cast(Integer, 1) == {:ok, 1}

    assert dump(Integer, {:increment, 1}) == {:ok, {:increment, 1}}
    assert cast(Integer, {:increment, 2}) == {:ok, {:increment, 2}}

    assert load(Integer, "a") == :error
    assert dump(Integer, "a") == :error
    assert cast(Integer, "a") == :error
  end
end

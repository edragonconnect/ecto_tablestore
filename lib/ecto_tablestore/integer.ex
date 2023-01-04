defmodule EctoTablestore.Integer do
  @moduledoc false
  use Ecto.Type

  def type, do: :integer

  # cast

  def cast({:increment, int}) when is_integer(int) do
    {:ok, {:increment, int}}
  end

  def cast(int), do: Ecto.Type.cast(:integer, int)

  # load

  def load(term) when is_integer(term), do: {:ok, term}
  def load(_term), do: :error

  # dump

  def dump({:increment, int}) when is_integer(int) do
    {:ok, {:increment, int}}
  end

  def dump(int) when is_integer(int), do: {:ok, int}
  def dump(_), do: :error
end

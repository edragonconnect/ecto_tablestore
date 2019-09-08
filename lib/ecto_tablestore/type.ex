defmodule EctoTablestore.Integer do
  @behaviour Ecto.Type

  def type, do: :integer

  def cast(int) do
    {:ok, int}
  end

  def load(term) when is_integer(term) do
    {:ok, term}
  end

  def load(_term) do
    :error
  end

  def dump({:increment, int}) when is_integer(int) do
    {:ok, {:increment, int}}
  end

  def dump(int) when is_integer(int) do
    {:ok, int}
  end

  def dump(_), do: :error
end

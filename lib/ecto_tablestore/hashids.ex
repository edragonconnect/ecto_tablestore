if Code.ensure_loaded?(Hashids) do
  defmodule EctoTablestore.Hashids do
    @moduledoc false
    use Ecto.Type

    def type, do: :binary_id
  
    def cast(id) when is_bitstring(id) do
      {:ok, id}
    end
    def cast(_), do: :error

    def load(term) when is_bitstring(term) do
      {:ok, term}
    end
    def load(_), do: :error

    def dump(term) do
      {:ok, term}
    end
  end

end

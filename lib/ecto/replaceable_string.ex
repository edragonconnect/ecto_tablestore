defmodule Ecto.ReplaceableString do
  use Ecto.ParameterizedType

  @doc false
  @impl true
  def type(_params), do: :string

  @doc false
  @impl true
  def init(opts) do
    opts
    |> Keyword.take([:on_dump, :on_load])
    |> Enum.reduce(%{}, fn({key, opts}, acc) -> 
      prepare_init(key, opts, acc)
    end)
  end

  @doc false
  @impl true
  def cast(nil, _params), do: {:ok, nil}
  def cast(data, _params) when not is_bitstring(data), do: :error
  def cast(data, _params) do
    {:ok, data}
  end

  @doc false
  @impl true
  def load(nil, _loader, _params), do: {:ok, nil}
  def load(data, _loader, %{on_load: %{pattern: pattern, replacement: replacement} = on_load}) do
    data = String.replace(data, pattern, replacement, Map.get(on_load, :options, []))
    {:ok, data}
  end
  def load(data, _loader, _params), do: {:ok, data}

  @doc false
  @impl true
  def dump(nil, _dumper, _params), do: {:ok, nil}
  def dump(data, _dumper, %{on_dump: %{pattern: pattern, replacement: replacement} = on_dump}) do
    data = String.replace(data, pattern, replacement, Map.get(on_dump, :options, []))
    {:ok, data}
  end
  def dump(data, _dumper, _params), do: {:ok, data}

  @doc false
  @impl true
  def embed_as(_format, _params) do
    # make sure `load/3` will be invoked when use `on_load` case.
    :dump
  end

  defp prepare_init(_key, nil, acc), do: acc
  defp prepare_init(key, opts, acc) do
    opts
    |> Keyword.take([:pattern, :replacement, :options])
    |> Enum.into(%{})
    |> validate(key, acc)
  end

  defp validate(opts, _key, acc) when opts === %{} do
    acc
  end
  defp validate(%{pattern: pattern, replacement: replacement} = opts, key, acc)
       when pattern != nil and is_bitstring(replacement) do
    Map.put(acc, key, opts)
  end
  defp validate(opts, key, _acc) do
    raise ArgumentError, """
    #{inspect(__MODULE__)} type must both have a `:pattern` option specified as a string or a regex
    type and a `:replacement` option as a string type for `#{key}` key,
    but got `#{inspect(opts)}`, they are same to String.replace/4.

    For example:

        field :my_field, EctoTablestore.Replaceable, on_dump:
          pattern: "a,b,c", replacement: ","

    or

        field :my_field, EctoTablestore.Replaceable, on_dump:
          pattern: "a,b,c", replacement: ",", options: [global: false]
    """
  end
end

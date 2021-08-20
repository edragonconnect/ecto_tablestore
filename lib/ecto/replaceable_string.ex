defmodule Ecto.ReplaceableString do
  @moduledoc """
  A custom type that replace some terms for strings.

  `#{inspect(__MODULE__)}` can be used when you want to replace some terms on dump/load
  a field, or even both included, for example:

  Before a field persisted to the database, `#{inspect(__MODULE__)}` replaces a matched pattern
  with the replacement, the final replaced value will be saved into database.

      field :content, #{inspect(__MODULE__)},
        on_dump: pattern: "hello", replacement: "hi", options: [global: false]

  After a field loaded from the database, `#{inspect(__MODULE__)}` replaces a matched pattern
  with the replacement, the final replaced value will extracted into the struct, but
  no changes into the original database.

      field :content, #{inspect(__MODULE__)},
        on_load: pattern: ~r/test/, replacement: "TEST"

  The `:pattern` and `:replacement` options are a pair required when both existed, the `:options` option
  is optional, these three options are correspond completely to `String.replace/4`.

  If no `:on_dump` or `:on_load` option(s) set, the following cases are equal from result perspective, but
  recommend to use the `:string` type as a base type:

      field :content, #{inspect(__MODULE__)}
      # equals
      field :content, :string

  Please notice that once used this type means a string replacement will be invoked in each read or write operation
  to the corresponding field, so please ensure use this type in a proper scenario to avoid a wasted performance issue,
  or find a better way to satisfy the similar use case.
  """
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

  @doc false
  @impl true
  def equal?(a, b, _params), do: a == b

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

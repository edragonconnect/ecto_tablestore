if Code.ensure_loaded?(Hashids) do
  defmodule Ecto.Hashids do
    @moduledoc """
    A cutom type leverages the generated sequence integer value to
    encode a hashid string for the primary key, recommend to use this for
    the partition key (the first defined primary key).

    Define `:hashids` type to the primary key of schema, for example:

        defmodule MySchema do
          use EctoTablestore.Schema

          schema "table_name" do
            field(:id, Ecto.Hashids, primary_key: true, autogenerate: true)
          end

        end

    In the above case, there will try to find a schema `:hashids` configuration from
    the application environment:

        config :ecto_tablestore,
          hashids: [
            {MySchema, [salt: "...", min_len: ..., alphabet: "..."]}
          ]

    If the `:hashids` option of the application environment is not defined, there will use default
    options("`[]`") to `Hashids.new/1`.

    We can also set the options when define the `:hashids` type to the primary key,
    for example:

        field(:id, :hashids,
          primary_key: true,
          autogenerate: true
          hashids: [salt: "...", min_len: ..., alphabet: "..."]
        )

    Once the above `:hashids` options defined in the `:id` field of schema, there will always use them
    to encode a hashid, meanwhile the application runtime environment definition will not affect them.

    Please see the options of `Hashids.new/1` for details.
    """

    use Ecto.ParameterizedType

    @hashids_opts [:alphabet, :min_len, :salt]

    @impl true
    def type(_options) do
      # define type as `:binary_id` there will be processed as
      # `autogenerate_id` by Ecto, and can properly return the primary key
      # with the expected value into the struct after insert.
      :binary_id
    end

    @impl true
    def init(opts) do
      opts
      |> Keyword.get(:hashids, [])
      |> Keyword.take(@hashids_opts)
      |> Keyword.put(:schema, opts[:schema])
    end

    @impl true
    def cast(value, _options) when is_bitstring(value) do
      {:ok, value}
    end

    @impl true
    def load(value, _loader, _options) do
      {:ok, value}
    end

    @impl true
    def dump(nil, _, _), do: {:ok, nil}
    def dump(value, _dumper, options) when is_integer(value) and value >= 0 do
      value = options |> new_hashids() |> Hashids.encode(value)
      {:ok, value}
    end
    def dump(value, _dumper, _options) when is_bitstring(value) do
      # already hashids encoded case but this dump callback still invoked by Ecto
      # for example, insert and then update it with an generated hashids
      {:ok, value}
    end

    defp new_hashids(opts) do
      hashids_opts = Keyword.take(opts, @hashids_opts)

      if hashids_opts == [] do
        opts[:schema]
        |> fetch_env_hashids_opts!()
        |> opts_to_hashids()
      else
        opts_to_hashids(hashids_opts)
      end
    end

    defp fetch_env_hashids_opts!(schema) do
      opts =
        :ecto_tablestore
        |> Application.get_env(:hashids, [])
        |> Keyword.get(schema, [])

      if not is_list(opts),
        do: raise_invalid_opts(opts)

      opts
    end

    defp opts_to_hashids(opts) when is_list(opts) do
      Hashids.new(opts)
    end
    defp opts_to_hashids(opts) do
      raise_invalid_opts(opts)
    end

    defp raise_invalid_opts(opts) do
      raise ArgumentError, message: "expect a keyword as options to Hashids.new/1, but got: #{inspect(opts)}"
    end
  end
end

defmodule EctoTablestore.Schema do
  @moduledoc ~S"""
  Defines a schema for Tablestore.

  An Ecto schema is used to map any data source into an Elixir struct. The definition of the
  schema is possible through the API: `tablestore_schema/2`.

  `tablestore_schema/2` is typically used to map data from a persisted source, usually a
  Tablestore table, into Elixir structs and vice-versa. For this reason, the first argument of
  `tablestore_schema/2` is the source(table) name. Structs defined with `tablestore_schema/2` also
  contain a `__meta__` field with metadata holding the status of struct, for example, if it has
  bee built, loaded or deleted.

  Since Tablestore is a NoSQL database service, `embedded_schema/1` is not supported so far.

  ## About timestamps

  Since Tablestore's column does not support `DateTime` type, use UTC timestamp (:integer type) as
  `timestamps()` macro for the generated `inserted_at` and `updated_at` fields by default.

  ## About primary key

  * The primary key supports `:id` (integer()) and `:binary_id` (binary()).
  * By default the `:primary_key` option is `false`.
  * The first defined primary key by the written order in the `tablestore_schema` is the partition
    key.
  * Up to 4 primary key(s), it is limited by TableStore product server side.
  * Up to 1 primary key with `autogenerate: true` option, it is limited by TableStore product
    server side.
  * The primary key set with `autogenerate: true` will use the TableStore product server's
    AUTO_INCREMENT feature.
  * If the partition key set as `autogenerate: true` is not allowed to take advantage of the
    AUTO_INCREMENT feature which it is limited by server, but there is a built-in implement to use
    the `Sequence` to achieve the same atomic increment operation in `ecto_tablestore` library.

  ## Example

      defmodule User do
        use EctoTablestore.Schema

        tablestore_schema "users" do
          field :outer_id, :binary_id, primary_key: true
          field :internal_id, :id, primary_key: true, autogenerate: true
          field :name, :string
          field :desc
        end
      end

  By default, if not explicitly set field type will process it as `:string` type.
  """

  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      import EctoTablestore.Schema, only: [tablestore_schema: 2]

      @primary_key false
      @timestamps_opts [
        type: :integer,
        autogenerate: {EctoTablestore.Schema, :__timestamps__, []}
      ]
    end
  end

  def __timestamps__() do
    DateTime.utc_now() |> DateTime.to_unix()
  end

  defmacro tablestore_schema(source, do: block) do
    {block, hashids} = check_block(block, __CALLER__.module)
    quote do
      Ecto.Schema.schema(unquote(source), do: unquote(block))

      unquote(generate_hashids_config(hashids))
    end
  end

  defp generate_hashids_config(hashids) do
    for {key, {opts, schema_module}} <- hashids do
      quote location: :keep do
        def hashids(unquote(key)) do
          schema_module = unquote(schema_module)
          opts = unquote(opts)

          opts = fetch_hashids_opts(opts, schema_module)

          if not is_list(opts) do
            raise "Using invalid options: #{inspect(opts)} for `#{schema_module}` schema, please check it should be a keyword."
          end

          opts
          |> Keyword.take([:salt, :min_len, :alphabet])
          |> Hashids.new()
        end

        defp fetch_hashids_opts(nil, schema_module) do
          Application.fetch_env!(:ecto_tablestore, :hashids) |> Keyword.get(schema_module, [])
        end

        defp fetch_hashids_opts(opts, _schema_module) do
          opts
        end

      end
    end
  end

  defp check_block({:__block__, info, fields}, schema_module) do
    {fields, hashids} = supplement_fields(fields, [], [], schema_module)

    {
      {:__block__, info, fields},
      Macro.escape(hashids)
    }
  end

  defp check_block(block, _) do
    block
  end

  defp supplement_fields([], prepared, hashids, _schema_module) do
    {Enum.reverse(prepared), hashids}
  end

  defp supplement_fields(
         [
           {defined_macro, field_line, [field_name, :hashids, opts]} = field
           | rest_fields
         ],
         prepared,
         prepared_hashids,
         schema_module
       ) do
    if Keyword.get(opts, :primary_key, false) do
      {field, new_hashids} =
        supplement_hashids_field(defined_macro, field_line, field_name, opts, schema_module)

      supplement_fields(
        rest_fields,
        [field | prepared],
        [new_hashids | prepared_hashids],
        schema_module
      )
    else
      supplement_fields(rest_fields, [field | prepared], prepared_hashids, schema_module)
    end
  end

  defp supplement_fields(
         [
           {defined_macro, field_line, [field_name, {:__aliases__, _line, type}, opts]} = field
           | rest_fields
         ],
         prepared,
         prepared_hashids,
         schema_module
       )
       when type == [:EctoTablestore, :Hashids]
       when type == [:Hashids] do
    if Keyword.get(opts, :primary_key, false) do
      {field, new_hashids} =
        supplement_hashids_field(defined_macro, field_line, field_name, opts, schema_module)

      supplement_fields(
        rest_fields,
        [field | prepared],
        [new_hashids | prepared_hashids],
        schema_module
      )
    else
      supplement_fields(rest_fields, [field | prepared], prepared_hashids, schema_module)
    end
  end

  defp supplement_fields(
         [{defined_macro, field_line, field_info} | rest_fields],
         prepared,
         hashids,
         schema_module
       ) do
    supplement_fields(
      rest_fields,
      [{defined_macro, field_line, field_info} | prepared],
      hashids,
      schema_module
    )
  end

  defp supplement_hashids_field(defined_macro, field_line, field_name, opts, schema_module) do

    field = {
      defined_macro,
      field_line,
      [
        field_name,
        EctoTablestore.Hashids,
        opts
      ]
    }

    {field, {field_name, {opts[:hashids], schema_module}}}
  end

end

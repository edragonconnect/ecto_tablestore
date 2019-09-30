defmodule Ecto.Adapters.Tablestore do
  @moduledoc false
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema

  alias __MODULE__

  alias EctoTablestore.Sequence
  alias ExAliyunOts.Mixin, as: TablestoreMixin

  alias ExAliyunOts.Const.{
    PKType,
    ReturnType,
    FilterType,
    LogicOperator,
    RowExistence,
    OperationType
  }

  require PKType
  require ReturnType
  require FilterType
  require LogicOperator
  require RowExistence
  require OperationType

  require Logger

  @error_condition_check_fail ~r/ConditionCheckFail/

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      ## Query

      @spec search(schema :: Ecto.Schema.t(), index_name :: String.t(), options :: Keyword.t()) ::
              {:ok, EctoTablestore.Repo.search_result} | {:error, term()}
      def search(schema, index_name, options) do
        Tablestore.search(get_dynamic_repo(), schema, index_name, options)
      end

      @spec one(entity :: Ecto.Schema.t(), options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil
      def one(%{__meta__: meta} = entity, options \\ []) do
        options = Tablestore.generate_filter_options(entity, options)
        get(meta.schema, Ecto.primary_key(entity), options)
      end

      @spec get(schema :: Ecto.Schema.t(), ids :: list, options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil
      def get(schema, ids, options \\ []) do
        Tablestore.get(get_dynamic_repo(), schema, ids, options)
      end

      @spec get_range(
              schema :: Ecto.Schema.t(),
              start_primary_keys :: list | binary(),
              end_primary_keys :: list,
              options :: Keyword.t()
            ) :: {list, nil} | {list, binary()} | {:error, term()}
      def get_range(
            schema,
            start_primary_keys,
            end_primary_keys,
            options \\ [direction: :forward]
          ) do
        Tablestore.get_range(
          get_dynamic_repo(),
          schema,
          start_primary_keys,
          end_primary_keys,
          options
        )
      end

      @spec batch_get(gets) ::
              {:ok, Keyword.t()} | {:error, term()}
            when gets: [
                   {
                     module :: Ecto.Schema.t(),
                     [{key :: String.t() | atom(), value :: integer | String.t()}],
                     options :: Keyword.t()
                   }
                   | {
                       module :: Ecto.Schema.t(),
                       [{key :: String.t() | atom(), value :: integer | String.t()}]
                     }
                   | (schema_entity :: Ecto.Schema.t())
                   | {[schema_entity :: Ecto.Schema.t()], options :: Keyword.t()}
                 ]
      def batch_get(gets) do
        Tablestore.batch_get(
          get_dynamic_repo(),
          gets
        )
      end

      ## Addition

      @spec batch_write(writes) ::
              {:ok, Keyword.t()} | {:error, term()}
            when writes: [
                   {
                     operation :: :put,
                     items :: [
                       schema_entity ::
                         Ecto.Schema.t()
                         | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
                         | {module :: Ecto.Schema.t(), ids :: list(), attrs :: list(),
                            options :: Keyword.t()}
                     ]
                   }
                   | {
                       operation :: :update,
                       items :: [
                         changeset ::
                           Ecto.Changeset.t()
                           | {changeset :: Ecto.Changeset.t(), options :: Keyword.t()}
                       ]
                     }
                   | {
                       operation :: :delete,
                       items :: [
                         schema_entity ::
                           Ecto.Schema.t()
                           | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
                           | {module :: Ecto.Schema.t(), ids :: list(), options :: Keyword.t()}
                       ]
                     }
                 ]
      def batch_write(writes) do
        Tablestore.batch_write(
          get_dynamic_repo(),
          writes
        )
      end

    end
  end

  @impl true
  def checkout(_adapter_meta, _config, function), do: function.()

  @impl true
  def dumpers(_primitive, :integer), do: [EctoTablestore.Integer]
  def dumpers(_primitive, type), do: [type]

  @impl true
  def loaders(_primitive, type), do: [type]

  @impl true
  def ensure_all_started(_config, _type), do: {:ok, [:ex_aliyun_ots, :ecto_tablestore]}

  @impl true
  def init(config) do
    Logger.info("init with config: #{inspect(config)}")

    case Keyword.get(config, :instance, nil) do
      tablestore_instance when is_atom(tablestore_instance) ->
        {
          :ok,
          Supervisor.Spec.supervisor(Supervisor, [[], [strategy: :one_for_one]]),
          %{instance: tablestore_instance}
        }

      nil ->
        raise "Missing `instance` option in EctoTablestore.Repo configuration properly, or found its value is not an :atom."
    end
  end

  ## Schema

  @impl true
  def autogenerate(:id), do: nil
  def autogenerate(:binary_id), do: nil

  @impl true
  def insert(repo, schema_meta, fields, _on_conflict, _returning, options) do

    schema = schema_meta.schema

    {pks, attrs, autogenerate_id_name} = pks_and_attrs_to_put_row(repo, schema, fields)

    result =
      TablestoreMixin.execute_put_row(
        repo.instance,
        schema_meta.source,
        pks,
        attrs,
        options
      )

    case result do
      {:ok, response} ->
        case response.row do
          {pks, _attrs} ->
            if autogenerate_id_name == nil do
              {:ok, []}
            else
              {_, autogenerate_value} = List.keyfind(pks, Atom.to_string(autogenerate_id_name), 0)
              {:ok, [{autogenerate_id_name, autogenerate_value}]}
            end

          nil ->
            {:ok, []}
        end

      {:error, error} ->
        Logger.error("ecto_tablestore insert - occur error: #{inspect(error)}")
        {:invalid, [{:error, error}]}
    end
  end

  @impl true
  def delete(repo, schema_meta, filters, options) do

    result =
      TablestoreMixin.execute_delete_row(
        repo.instance,
        schema_meta.source,
        format_key_to_str(filters),
        options
      )

    case result do
      {:ok, _response} ->
        {:ok, []}

      _error ->
        {:error, message} = result

        cond do
          String.match?(message, @error_condition_check_fail) ->
            {:error, :stale}

          true ->
            {:invalid, [result]}
        end
    end
  end

  @impl true
  def update(repo, schema_meta, fields, filters, _returning, options) do

    schema = schema_meta.schema

    result =
      TablestoreMixin.execute_update_row(
        repo.instance,
        schema_meta.source,
        format_key_to_str(filters),
        Keyword.merge(options, map_attrs_to_update(schema, fields))
      )

    case result do
      {:ok, response} ->
        case response.row do
          nil ->
            {:ok, []}

          _ ->
            {:ok, extract_as_keyword(schema, response.row)}
        end

      _error ->
        {:invalid, [result]}
    end
  end

  @impl true
  def insert_all(repo, schema_meta, header, list, on_conflict, returning, options) do
    IO.puts(
      "insert_all - repo: #{inspect(repo)}, schema_meta: #{inspect(schema_meta)}, header: #{
        inspect(header)
      }, list: #{inspect(list)}, on_conflict: #{inspect(on_conflict)}, returning: #{
        inspect(returning)
      }, options: #{inspect(options)}\nplease use `batch_write` instead this function"
    )
  end

  @doc false
  def search(repo, schema, index_name, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    result =
      TablestoreMixin.execute_search(
        meta.instance,
        schema.__schema__(:source),
        index_name,
        options
      )

    case result do
      {:ok, response} ->
        {
          :ok,
          %{
            is_all_succeeded: response.is_all_succeeded,
            next_token: response.next_token,
            total_hits: response.total_hits,
            schemas: Enum.map(response.rows, fn(row) ->
              row_to_schema(schema, row)
            end)
          }
        }
      _error ->
        result
    end
  end

  @doc false
  def get(repo, schema, ids, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    result =
      TablestoreMixin.execute_get_row(
        meta.instance,
        schema.__schema__(:source),
        format_key_to_str(ids),
        options
      )

    case result do
      {:ok, response} ->
        row_to_schema(schema, response.row)

      _error ->
        result
    end
  end

  @doc false
  def get_range(repo, schema, start_primary_keys, end_primary_keys, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    prepared_start_primary_keys =
      cond do
        is_list(start_primary_keys) ->
          format_key_to_str(start_primary_keys)

        is_binary(start_primary_keys) ->
          start_primary_keys

        true ->
          raise "Invalid start_primary_keys: #{inspect(start_primary_keys)}, expect it as `list` or `binary`"
      end

    result =
      TablestoreMixin.execute_get_range(
        meta.instance,
        schema.__schema__(:source),
        prepared_start_primary_keys,
        format_key_to_str(end_primary_keys),
        options
      )

    case result do
      {:ok, response} ->
        records =
          Enum.map(response.rows, fn row ->
            row_to_schema(schema, row)
          end)

        {records, response.next_start_primary_key}

      _error ->
        result
    end
  end

  @doc false
  def batch_get(repo, gets) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    {requests, schemas_mapping} = Enum.reduce(gets, {[], %{}}, &map_batch_gets/2)

    prepared_requests = Enum.reverse(requests)

    result = TablestoreMixin.execute_batch_get(meta.instance, prepared_requests)

    case result do
      {:ok, response} ->
        {
          :ok,
          batch_get_row_response_to_schemas(response.tables, schemas_mapping)
        }

      _error ->
        result
    end
  end

  @doc false
  def batch_write(repo, writes) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    {var_write_requests, schema_entities_map} =
      writes
      |> Enum.map(&map_batch_writes(repo, &1))
      |> Enum.unzip()

    {prepared_requests, operations} =
      var_write_requests
      |> reduce_merge_map()
      |> Enum.reduce({[], %{}}, fn {source, requests}, {requests_acc, operations_acc} ->
        operations =
          for req <- requests do
            case req.type do
              OperationType.delete() -> :delete
              OperationType.update() -> :update
              OperationType.put() -> :put
            end
          end

        {
          [{source, requests} | requests_acc],
          Map.put(operations_acc, source, operations)
        }
      end)

    input_schema_entities = reduce_merge_map(schema_entities_map)

    result = TablestoreMixin.execute_batch_write(meta.instance, prepared_requests, [])

    case result do
      {:ok, response} ->
        {
          :ok,
          batch_write_row_response_to_schemas(response.tables, input_schema_entities, operations)
        }

      _error ->
        result
    end
  end

  @doc false
  def generate_condition_options(%{__meta__: _meta} = entity, options) do
    condition =
      entity
      |> generate_filter_options([])
      |> do_generate_condition(Keyword.get(options, :condition))

    Keyword.put(options, :condition, condition)
  end

  @doc false
  def generate_filter_options(%{__meta__: _meta} = entity, options) do
    attr_columns = entity_attr_columns(entity)

    options =
      attr_columns
      |> generate_filter_ast([])
      |> do_generate_filter_options(options)

    columns_to_get_opt = Keyword.get(options, :columns_to_get, [])

    if not is_list(columns_to_get_opt),
      do:
        raise(
          "Invalid usecase - require `columns_to_get` as list, but get: #{
            inspect(columns_to_get_opt)
          }"
        )

    if columns_to_get_opt == [] do
      options
    else
      implicit_columns_to_get =
        attr_columns
        |> Keyword.keys()
        |> Enum.map(fn field_name ->
          Atom.to_string(field_name)
        end)

      updated_columns_to_get =
        (implicit_columns_to_get ++ columns_to_get_opt)
        |> MapSet.new()
        |> MapSet.to_list()

      Keyword.put(options, :columns_to_get, updated_columns_to_get)
    end
  end

  @doc false
  def execute_ddl(repo, definition) do
    repo
    |> Ecto.Adapter.lookup_meta()
    |> do_execute_ddl(definition)
  end

  @doc false
  def table_sequence_name(table_name) do
    "#{table_name}_seq"
  end

  defp do_generate_condition([], nil) do
    nil
  end

  defp do_generate_condition([], %ExAliyunOts.Var.Condition{} = condition_opt) do
    condition_opt
  end

  defp do_generate_condition([filter: filter_from_entity], nil) do
    %ExAliyunOts.Var.Condition{
      column_condition: filter_from_entity,
      row_existence: RowExistence.expect_exist()
    }
  end

  defp do_generate_condition([filter: filter_from_entity], %ExAliyunOts.Var.Condition{
         column_condition: nil
       }) do
    %ExAliyunOts.Var.Condition{
      column_condition: filter_from_entity,
      row_existence: RowExistence.expect_exist()
    }
  end

  defp do_generate_condition([filter: filter_from_entity], %ExAliyunOts.Var.Condition{
         column_condition: column_condition
       }) do
    %ExAliyunOts.Var.Condition{
      column_condition: do_generate_filter(filter_from_entity, :and, column_condition),
      row_existence: RowExistence.expect_exist()
    }
  end

  defp do_generate_filter_options(nil, options) do
    options
  end

  defp do_generate_filter_options(ast, options) do
    merged =
      ast
      |> ExAliyunOts.Mixin.expressions_to_filter(binding())
      |> do_generate_filter(:and, Keyword.get(options, :filter))

    Keyword.put(options, :filter, merged)
  end

  defp do_generate_filter(filter_from_entity, :and, nil) do
    filter_from_entity
  end

  defp do_generate_filter(filter_from_entity, :and, %ExAliyunOts.Var.Filter{} = filter_from_opt) do
    %ExAliyunOts.Var.Filter{
      filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
        combinator: LogicOperator.and(),
        sub_filters: [filter_from_entity, filter_from_opt]
      },
      filter_type: FilterType.composite_column()
    }
  end

  defp do_generate_filter(filter_from_entity, :or, %ExAliyunOts.Var.Filter{} = filter_from_opt) do
    %ExAliyunOts.Var.Filter{
      filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
        combinator: LogicOperator.or(),
        sub_filters: [filter_from_entity, filter_from_opt]
      },
      filter_type: FilterType.composite_column()
    }
  end

  defp do_generate_filter(_filter_from_entity, :and, filter_from_opt) do
    raise("Invalid usecase - input invalid `:filter` option: #{inspect(filter_from_opt)}")
  end

  defp generate_filter_ast([], prepared) do
    List.first(prepared)
  end

  defp generate_filter_ast([{field_name, value} | rest], []) when is_atom(field_name) do
    field_name = Atom.to_string(field_name)
    ast = quote do: unquote(field_name) == unquote(value)
    generate_filter_ast(rest, [ast])
  end

  defp generate_filter_ast([{field_name, value} | rest], prepared) when is_atom(field_name) do
    field_name = Atom.to_string(field_name)
    ast = quote do: unquote(field_name) == unquote(value)
    generate_filter_ast(rest, [{:and, [], [ast | prepared]}])
  end

  defp pks_and_attrs_to_put_row(repo, schema, fields) do
    primary_keys = schema.__schema__(:primary_key)
    autogenerate_id = schema.__schema__(:autogenerate_id)
    map_pks_and_attrs_to_put_row(primary_keys, autogenerate_id, fields, [], repo, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         [],
         nil,
         attr_columns,
         prepared_pks,
         _repo,
         schema
       ) do
    attrs = map_attrs_to_row(schema, attr_columns)

    {
      Enum.reverse(prepared_pks),
      attrs,
      nil
    }
  end

  defp map_pks_and_attrs_to_put_row(
         [],
         {_, autogenerate_id_name, :id},
         attr_columns,
         prepared_pks,
         _repo,
         schema
       ) do
    attrs = map_attrs_to_row(schema, attr_columns)

    {
      Enum.reverse(prepared_pks),
      attrs,
      autogenerate_id_name
    }
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         nil,
         fields,
         prepared_pks,
         repo,
         schema
       ) do

    {value, updated_fields} = Keyword.pop(fields, primary_key)
    if value == nil,
      do: raise("Invalid usecase - primary key: `#{primary_key}` can not be nil.")

    update = [{Atom.to_string(primary_key), value} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, nil, updated_fields, update, repo, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks,
         repo,
         schema
       )
       when primary_key == autogenerate_id_name and prepared_pks == [] do
    # Set partition_key as auto-generated, use sequence for this usecase

    source = schema.__schema__(:source)
    field_name_str = Atom.to_string(autogenerate_id_name)

    next_value = Sequence.next_value(repo.instance, seq_name_to_tab(source), field_name_str)

    update = [{field_name_str, next_value} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, autogenerate_id, fields, update, repo, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks,
         repo,
         schema
       )
       when primary_key == autogenerate_id_name do
    update = [{Atom.to_string(autogenerate_id_name), PKType.auto_increment()} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, autogenerate_id, fields, update, repo, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks,
         repo,
         schema
       )
       when primary_key != autogenerate_id_name do

    {value, updated_fields} = Keyword.pop(fields, primary_key)

    IO.puts "fields: #{inspect fields}, primary_key: #{inspect primary_key}, autogenerate_id_name: #{autogenerate_id_name}"

    if value == nil,
      do: raise("Invalid usecase - autogenerate primary key: `#{primary_key}` can not be nil.")

    update = [{Atom.to_string(primary_key), value} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, autogenerate_id, updated_fields, update, repo, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         _,
         {_, _autogenerate_id_name, :binary_id},
         _fields,
         _prepared_pks,
         _repo,
         _schema
       ) do
    raise "Not support autogenerate id as string (`:binary_id`)."
  end

  defp map_attrs_to_row(schema, attr_columns) do
    for {field, value} <- attr_columns do
      field_type = schema.__schema__(:type, field)
      do_map_attr_to_row_item(field_type, field, value)
    end
  end

  defp do_map_attr_to_row_item(type, key, value)
        when type == :naive_datetime_usec
        when type == :naive_datetime do
    {Atom.to_string(key), DateTime.from_naive!(value, "Etc/UTC") |> DateTime.to_unix()}
  end
  defp do_map_attr_to_row_item(type, key, value)
        when type == :utc_datetime
        when type == :utc_datetime_usec do
    {Atom.to_string(key), DateTime.to_unix(value)}
  end
  defp do_map_attr_to_row_item(type, key, value)
        when type == :map
        when type == :array do
    {Atom.to_string(key), Jason.encode!(value)}
  end
  defp do_map_attr_to_row_item({:array, _}, key, value) do
    {Atom.to_string(key), Jason.encode!(value)}
  end
  defp do_map_attr_to_row_item({:map, _}, key, value) do
    {Atom.to_string(key), Jason.encode!(value)}
  end
  defp do_map_attr_to_row_item(_, key, value) do
    {Atom.to_string(key), value}
  end

  defp row_to_schema(_schema, nil) do
    nil
  end

  defp row_to_schema(schema, row) do
    struct(schema, extract_as_keyword(schema, row))
  end

  defp extract_as_keyword(schema, {nil, attrs}) do
    for {attr_key, attr_value, _ts} <- attrs do
      field = String.to_atom(attr_key)
      type = schema.__schema__(:type, field)
      do_map_row_item_to_attr(type, field, attr_value)
    end
  end

  defp extract_as_keyword(_schema, {pks, nil}) do
    for {pk_key, pk_value} <- pks do
      {String.to_atom(pk_key), pk_value}
    end
  end

  defp extract_as_keyword(schema, {pks, attrs}) do
    prepared_pks =
      for {pk_key, pk_value} <- pks do
        {String.to_atom(pk_key), pk_value}
      end

    prepared_attrs =
      for {attr_key, attr_value, _ts} <- attrs do
        field = String.to_atom(attr_key)
        type = schema.__schema__(:type, field)
        do_map_row_item_to_attr(type, field, attr_value)
      end

    Keyword.merge(prepared_attrs, prepared_pks)
  end

  defp do_map_row_item_to_attr(type, key, value)
        when type == :map and is_atom(key)
        when type == :array and is_atom(key) do
    {key, Jason.decode!(value)}
  end
  defp do_map_row_item_to_attr({:array, _}, key, value) when is_atom(key) do
    {key, Jason.decode!(value)}
  end
  defp do_map_row_item_to_attr({:map, _}, key, value) when is_atom(key) do
    {key, Jason.decode!(value)}
  end
  defp do_map_row_item_to_attr(_type, key, value) when is_atom(key) do
    {key, value}
  end

  defp map_attrs_to_update(schema, attrs) do
    {_, updates} = Enum.reduce(attrs, {schema, Keyword.new()}, &construct_row_updates/2)
    updates
  end

  defp construct_row_updates({field, nil}, {schema, acc}) when is_atom(field) do
    if Keyword.has_key?(acc, :delete_all) do
      {
        schema,
        Keyword.update!(acc, :delete_all, &[Atom.to_string(field) | &1])
      }
    else
      {
        schema,
        Keyword.put(acc, :delete_all, [Atom.to_string(field)])
      }
    end
  end

  defp construct_row_updates({field, {:increment, value}}, {schema, acc}) when is_integer(value) do
    field_str = Atom.to_string(field)

    updated_acc =
      if Keyword.has_key?(acc, :increment) do
        acc
        |> Keyword.update!(:increment, &[{field_str, value} | &1])
        |> Keyword.update!(:return_columns, &[field_str | &1])
      else
        acc
        |> Keyword.put(:increment, [{field_str, value}])
        |> Keyword.put(:return_type, ReturnType.after_modify())
        |> Keyword.put(:return_columns, [field_str])
      end

    {schema, updated_acc}
  end

  defp construct_row_updates({field, value}, {schema, acc}) when is_atom(field) do
    field_type = schema.__schema__(:type, field)
    if Keyword.has_key?(acc, :put) do
      {
        schema,
        Keyword.update!(acc, :put, &[do_map_attr_to_row_item(field_type, field, value) | &1])
      }
    else
      {
        schema,
        Keyword.put(acc, :put, [do_map_attr_to_row_item(field_type, field, value)])
      }
    end
  end

  defp format_key_to_str(items) do
    format_key_to_str(items, [])
  end

  defp format_key_to_str([], prepared) when is_list(prepared) do
    Enum.reverse(prepared)
  end

  defp format_key_to_str([{_key, nil} | rest], prepared) when is_list(prepared) do
    format_key_to_str(rest, prepared)
  end

  defp format_key_to_str([{key, value} | rest], prepared)
       when is_atom(key) and is_list(prepared) do
    update = [{Atom.to_string(key), map_key_value(value)} | prepared]
    format_key_to_str(rest, update)
  end

  defp format_key_to_str([{key, value} | rest], prepared)
       when is_bitstring(key) and is_list(prepared) do
    update = [{key, map_key_value(value)} | prepared]
    format_key_to_str(rest, update)
  end

  defp format_key_to_str([item | _rest], _prepared) do
    raise "Invalid key item: #{inspect(item)}, expect its key as `:string` or `:atom`"
  end

  defp map_key_value(:inf_min), do: PKType.inf_min()
  defp map_key_value(:inf_max), do: PKType.inf_max()
  defp map_key_value(value), do: value

  defp map_batch_gets(schema_entities, acc) when is_list(schema_entities) do
    map_batch_gets({schema_entities, []}, acc)
  end

  defp map_batch_gets({schema_entities, options}, {requests, schemas_mapping})
       when is_list(schema_entities) do
    {ids_groups, source_set} =
      Enum.map_reduce(
        schema_entities,
        MapSet.new([]),
        fn %{__meta__: meta} = schema_entity, acc ->
          {
            format_key_to_str(Ecto.primary_key(schema_entity)),
            MapSet.put(acc, meta.schema)
          }
        end
      )

    if MapSet.size(source_set) > 1,
      do:
        raise(
          "Invalid usecase - input batch get request: #{inspect(schema_entities)} are different types of schema entity in batch."
        )

    {conflict_schemas, filters, columns_to_get} =
      Enum.reduce(schema_entities, {[], [], []}, fn schema_entity,
                                                    {conflict_schemas, filters, columns_to_get} ->
        opts = generate_filter_options(schema_entity, options)

        prepared_columns_to_get =
          (Keyword.get(opts, :columns_to_get, []) ++ columns_to_get)
          |> MapSet.new()
          |> MapSet.to_list()

        case Keyword.get(opts, :filter) do
          nil ->
            {[schema_entity | conflict_schemas], filters, prepared_columns_to_get}

          filter ->
            {conflict_schemas, [filter | filters], prepared_columns_to_get}
        end
      end)

    if length(filters) != 0 and length(conflict_schemas) != 0 do
      raise "Invalid usecase - conflicts for schema_entities: #{inspect(conflict_schemas)}, they are only be with primary key(s), but generate filters: #{
              inspect(filters)
            } from other schema_entities attribute fields, please input schema_entities both have attribute fields or use the `filter` option."
    end

    options =
      if filters != [] do
        filters =
          Enum.reduce(filters, fn filter, acc ->
            do_generate_filter(filter, :or, acc)
          end)

        Keyword.put(options, :filter, filters)
      else
        options
      end

    options =
      if columns_to_get != [] do
        Keyword.put(options, :columns_to_get, columns_to_get)
      else
        options
      end

    schema = MapSet.to_list(source_set) |> List.first()
    source = schema.__schema__(:source)

    request =
      TablestoreMixin.execute_get(
        source,
        ids_groups,
        options
      )

    {[request | requests], Map.put(schemas_mapping, source, schema)}
  end

  defp map_batch_gets({schema, ids_groups, options}, {requests, schemas_mapping})
       when is_list(ids_groups) do
    source = schema.__schema__(:source)

    request =
      TablestoreMixin.execute_get(
        source,
        format_ids_groups(ids_groups),
        options
      )

    {[request | requests], Map.put(schemas_mapping, source, schema)}
  end

  defp map_batch_gets({schema, ids_groups}, acc) when is_list(ids_groups) do
    map_batch_gets({schema, ids_groups, []}, acc)
  end

  defp map_batch_gets(request, _acc) do
    raise("Invalid usecase - input invalid batch get request: #{inspect(request)}")
  end

  defp map_batch_writes(_repo, {:delete, deletes}) do
    Enum.reduce(deletes, {%{}, %{}}, fn delete, {delete_acc, schema_entities_acc} ->
      {source, ids, options, schema_entity} = do_map_batch_writes(:delete, delete)
      write_delete_request = TablestoreMixin.execute_write_delete(ids, options)

      if Map.has_key?(delete_acc, source) do
        {
          Map.update!(delete_acc, source, &[write_delete_request | &1]),
          Map.update!(schema_entities_acc, source, &[schema_entity | &1])
        }
      else
        {
          Map.put(delete_acc, source, [write_delete_request]),
          Map.put(schema_entities_acc, source, [schema_entity])
        }
      end
    end)
  end

  defp map_batch_writes(repo, {:put, puts}) do
    Enum.reduce(puts, {%{}, %{}}, fn put, {puts_acc, schema_entities_acc} ->
      {source, ids, attrs, options, schema_entity} = do_map_batch_writes(:put, {repo, put})

      write_put_request = TablestoreMixin.execute_write_put(ids, attrs, options)

      if Map.has_key?(puts_acc, source) do
        {
          Map.update!(puts_acc, source, &[write_put_request | &1]),
          Map.update!(schema_entities_acc, source, &[schema_entity | &1])
        }
      else
        {
          Map.put(puts_acc, source, [write_put_request]),
          Map.put(schema_entities_acc, source, [schema_entity])
        }
      end
    end)
  end

  defp map_batch_writes(_repo, {:update, updates}) do
    Enum.reduce(updates, {%{}, %{}}, fn update, {update_acc, schema_entities_acc} ->
      {source, ids, options, schema_entity} = do_map_batch_writes(:update, update)

      write_update_request = TablestoreMixin.execute_write_update(ids, options)

      if Map.has_key?(update_acc, source) do
        {
          Map.update!(update_acc, source, &[write_update_request | &1]),
          Map.update!(schema_entities_acc, source, &[schema_entity | &1])
        }
      else
        {
          Map.put(update_acc, source, [write_update_request]),
          Map.put(schema_entities_acc, source, [schema_entity])
        }
      end
    end)
  end

  defp map_batch_writes(_repo, item) do
    raise("Invalid usecase - batch write with item: #{inspect(item)}")
  end

  defp do_map_batch_writes(:delete, %{__meta__: _meta} = schema_entity) do
    do_map_batch_writes(:delete, {schema_entity, []})
  end

  defp do_map_batch_writes(:delete, {%{__meta__: meta} = schema_entity, options}) do
    source = meta.schema.__schema__(:source)
    ids = Ecto.primary_key(schema_entity) |> format_key_to_str()
    options = generate_condition_options(schema_entity, options)
    {source, ids, options, schema_entity}
  end

  defp do_map_batch_writes(:delete, {schema, ids, options}) do
    source = schema.__schema__(:source)
    schema_entity = struct(schema, ids)
    ids = format_key_to_str(ids)
    {source, ids, options, schema_entity}
  end

  defp do_map_batch_writes(:put, {repo, %{__meta__: _meta} = schema_entity}) do
    do_map_batch_writes(:put, {repo, {schema_entity, []}})
  end

  defp do_map_batch_writes(:put, {repo, {%{__meta__: meta} = schema_entity, options}}) do
    schema = meta.schema
    
    source = schema.__schema__(:source)

    fields =
      schema_entity
      |> Ecto.primary_key()
      |> Enum.reduce(entity_attr_columns(schema_entity), fn {key, value}, acc ->
        if value != nil, do: [{key, value} | acc], else: acc
      end)

    autogen_fields = autogen_fields(schema)

    schema_entity =
      Enum.reduce(autogen_fields, schema_entity, fn({key, value}, acc) ->
        Map.put(acc, key, value)
      end)

    {pks, attrs, _autogenerate_id_name} = pks_and_attrs_to_put_row(repo, schema, Keyword.merge(autogen_fields, fields))
    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(:put, {repo, {schema, ids, attrs, options}}) do
    source = schema.__schema__(:source)
    autogen_fields = autogen_fields(schema)
    fields = Keyword.merge(autogen_fields, ids ++ attrs)
    schema_entity = struct(schema, fields)

    {pks, attrs, _autogenerate_id_name} = pks_and_attrs_to_put_row(repo, schema, fields)
    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(
         :update,
         {%Ecto.Changeset{valid?: true, data: %{__meta__: meta}} = changeset, options}
       ) do

    schema = meta.schema
    source = schema.__schema__(:source)
    entity = changeset.data
    ids = Ecto.primary_key(entity)

    options = generate_condition_options(entity, options)

    autoupdate_fields = autoupdate_fields(schema)

    changes = Enum.reduce(autoupdate_fields, changeset.changes, fn({key, value}, acc) ->
      Map.put(acc, key, value)
    end)

    update_attrs = map_attrs_to_update(schema, changes)

    schema_entity = do_map_merge(changeset.data, changes, true)

    {source, format_key_to_str(ids), merge_options(options, update_attrs), schema_entity}
  end

  defp do_map_batch_writes(
         :update,
         %Ecto.Changeset{valid?: true, data: %{__meta__: _meta}} = changeset
       ) do
    do_map_batch_writes(:update, {changeset, []})
  end

  defp do_map_batch_writes(:update, %Ecto.Changeset{valid?: false} = changeset) do
    raise "Using invalid changeset: #{inspect(changeset)} in batch writes"
  end

  defp format_ids_groups(ids_groups) do
    ids_groups
    |> Enum.map(fn ids_group ->
      if is_list(ids_group),
        do: format_key_to_str(ids_group),
        else: format_key_to_str([ids_group])
    end)
  end

  defp autogen_fields(schema) do
    case schema.__schema__(:autogenerate) do
      [{autogen_fields, {m, f, a}}] ->
        autogen_value = apply(m, f, a)
        Enum.map(autogen_fields, fn(f) -> {f, autogen_value} end)
      _ ->
        []
    end
  end

  defp autoupdate_fields(schema) do
    case schema.__schema__(:autoupdate) do
      [{autoupdate_fields, {m, f, a}}] ->
        autoupdate_value = apply(m, f, a)
        Enum.map(autoupdate_fields, fn(f) -> {f, autoupdate_value} end)
      _ ->
        []
    end
  end

  defp entity_attr_columns(%{__meta__: meta} = entity) do
    schema = meta.schema

    (schema.__schema__(:fields) -- schema.__schema__(:primary_key))
    |> Enum.reduce([], fn field_name, acc ->
      case Map.get(entity, field_name) do
        value when value != nil ->
          [{field_name, value} | acc]

        nil ->
          acc
      end
    end)
  end

  defp merge_options(input_opts, generated_opts) do
    Keyword.merge(input_opts, generated_opts, fn _option_name, input_v, generated_v ->
      do_merge_option(input_v, generated_v)
    end)
  end

  defp do_merge_option(input, generated) when input == nil and generated != nil, do: generated
  defp do_merge_option(input, generated) when input == nil and generated == nil, do: nil
  defp do_merge_option(input, generated) when input != nil and generated == nil, do: input

  defp do_merge_option(input, generated) when is_list(input) and is_list(generated),
    do: MapSet.new(input ++ generated) |> MapSet.to_list()

  defp do_merge_option(input, generated) when input == generated, do: input
  defp do_merge_option(_input, generated), do: generated

  defp batch_get_row_response_to_schemas(tables, schemas_mapping) do
    Enum.reduce(tables, [], fn table, acc ->
      schema = Map.get(schemas_mapping, table.table_name)

      schemas_data =
        Enum.reduce(table.rows, [], fn row_in_batch, acc ->
          schema_data = row_to_schema(schema, row_in_batch.row)
          if schema_data == nil, do: acc, else: [schema_data | acc]
        end)

      case schemas_data do
        [] ->
          Keyword.put(acc, schema, nil)

        _ ->
          Keyword.put(acc, schema, Enum.reverse(schemas_data))
      end
    end)
  end

  defp batch_write_row_response_to_schemas(tables, input_schema_entities, operations) do
    Enum.reduce(tables, [], fn table, acc ->
      source = table.table_name
      input_matched_schema_entities = Map.get(input_schema_entities, source)
      schema = List.first(input_matched_schema_entities).__meta__.schema

      results =
        operations
        |> Map.get(source, [])
        |> Enum.zip(Enum.zip(table.rows, input_matched_schema_entities))
        |> Enum.reduce([], fn {operation, {write_row_response, input}}, acc ->
          return =
            if write_row_response.is_ok == true do
              schema_entity_from_response = row_to_schema(schema, write_row_response.row)

              return_schema_entity = do_map_merge(input, schema_entity_from_response)

              {:ok, return_schema_entity}
            else
              {:error, write_row_response}
            end

          if Keyword.has_key?(acc, operation) do
            Keyword.update!(acc, operation, &[return | &1])
          else
            Keyword.put(acc, operation, [return])
          end
        end)

      Keyword.put(acc, schema, results)
    end)
  end

  defp reduce_merge_map(items) do
    Enum.reduce(items, %{}, fn map, acc ->
      Map.merge(acc, map, fn _key, v1, v2 ->
        v1 ++ v2
      end)
    end)
  end

  defp do_map_merge(map1, map_or_keyword, force_merge \\ false)
  defp do_map_merge(nil, map, _force_merge) when is_map(map) do
    map
  end
  defp do_map_merge(map, nil, _force_merge) when is_map(map) do
    map
  end
  defp do_map_merge(map1, map2, true) when is_map(map1) and is_map(map2) do
    Map.merge(map1, map2)
  end
  defp do_map_merge(map1, map2, false) when is_map(map1) and is_map(map2) do
    Map.merge(map1, map2, fn _k, v1, v2 ->
      if v2 != nil, do: v2, else: v1
    end)
  end
  defp do_map_merge(map, keyword, true) when is_map(map) and is_list(keyword) do
    Enum.reduce(keyword, map, fn({key, value}, acc) ->
      Map.put(acc, key, value)
    end)
  end
  defp do_map_merge(map, keyword, false) when is_map(map) and is_list(keyword) do
    Enum.reduce(keyword, map, fn({key, value}, acc) ->
      if value != nil, do: Map.put(acc, key, value), else: acc
    end)
  end

  ## Migration

  defp seq_name_to_tab(table_name) do
    "#{table_name}_seq"
  end

  defp do_execute_ddl(meta, {:create, table, columns}) do
    {table_name, primary_keys, create_seq?} = Enum.reduce(columns, {table.name, [], false}, &do_execute_ddl_add_column/2)

    Logger.info ">> table_name: #{table_name}, primary_keys: #{inspect primary_keys}, create_seq?: #{create_seq?}"

    instance = meta.instance

    result =
      TablestoreMixin.execute_create_table(
        meta.instance,
        table_name,
        primary_keys,
        Keyword.put(table.meta, :max_versions, 1)
      )

    Logger.info "create a table: #{table_name} result: #{inspect result}"

    if result == :ok and create_seq? do
      Sequence.create(instance, seq_name_to_tab(table_name))
    end
  end

  defp do_execute_ddl_add_column({:add, field_name, :integer, options}, {source, prepared_columns, create_seq?}) when is_atom(field_name) do

    partition_key? = Keyword.get(options, :partition_key, false)
    auto_increment? = Keyword.get(options, :auto_increment, false)

    {prepared_column, create_seq?} =
      cond do
        partition_key? == true and auto_increment? == true ->
          {
            {Atom.to_string(field_name), PKType.integer},
            true
          }
        create_seq? and auto_increment? == true ->
          raise Ecto.MigrationError, message:
            "the `auto_increment: true` option only allows binding of one primary key, but find the duplicated `#{field_name}` field with options: #{inspect options}"
        auto_increment? == true ->
          {
            {Atom.to_string(field_name), PKType.integer, PKType.auto_increment},
            create_seq?
          }
        true ->
          {
            {Atom.to_string(field_name), PKType.integer},
            create_seq?
          }
      end

    {
      source,
      prepared_columns ++ [prepared_column],
      create_seq?
    }
  end
  defp do_execute_ddl_add_column({:add, field_name, :string, _options}, {source, prepared_columns, create_seq?}) when is_atom(field_name) do
    {
      source,
      prepared_columns ++ [{Atom.to_string(field_name), PKType.string}],
      create_seq?
    }
  end
  defp do_execute_ddl_add_column({:add, field_name, :binary, _options}, {source, prepared_columns, create_seq?}) when is_atom(field_name) do
    {
      source,
      prepared_columns ++ [{Atom.to_string(field_name), PKType.binary}],
      create_seq?
    }
  end

end

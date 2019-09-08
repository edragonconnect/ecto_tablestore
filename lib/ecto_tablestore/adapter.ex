defmodule Ecto.Adapters.Tablestore do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema

  alias ExAliyunOts.Mixin, as: Tablestore

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

      @doc """
      Similar to `get/3` but use schema entity which has been filled the whole primary key(s).

      Notice:

      If there are some attribute column(s) are provided in entity, these fields will be combined within multiple `:==` filtering expressions;
      If there are some attribute column(s) are provided and meanwhile set `filter` option, they will be merged into a composite filter.

      ## Options

      Please refer `get/3`.
      """
      @spec one(entity :: Ecto.Schema.t(), options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil
      def one(%{__meta__: meta} = entity, options \\ []) do
        options = Ecto.Adapters.Tablestore.generate_filter_options(entity, :and, options)
        get(meta.schema, Ecto.primary_key(entity), options)
      end

      @doc """
      Fetch a single struct from tablestore where the whole primary key(s) match the given ids.

      ## Options

      * `columns_to_get`, string list, return the specified attribute columns, if not specify this field all attribute columns will be return.
      * `start_column`, string, used as a starting column for Wide Column read, the return result contains this as starter.
      * `end_column`, string, used as a ending column for Wide Column read, the return result DON NOT contain this column.
      * `filter`, used as a filter by condition, support `">"`, `"<"`, `">="`, `"<="`, `"=="`, `"and"`, `"or"` and `"()"` expressions.

          The `ignore_if_missing` can be used for the non-existed attribute column, for example:

          An attribute column does not exist meanwhile set it as `true`, will ignore this match condition in the return result;

          An existed attribute column DOES NOT suit for this usecase, the match condition will always affect the return result, if match condition does not satisfy, they won't be
          return in result.

          ```elixir
          filter: filter(("name[ignore_if_missing: true]" == var_name and "age" > 1) or ("class" == "1"))
          ```

      * `transaction_id`, read under local transaction in a partition key.
      """
      @spec get(schema :: Ecto.Schema.t(), ids :: list, options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil
      def get(schema, ids, options \\ []) do
        Ecto.Adapters.Tablestore.get(get_dynamic_repo(), schema, ids, options)
      end

      @doc """
      Get multiple structs by range from one table, rely on the conjunction of the partition key and other primary key(s). 

      ## Options

        * `direction`, set it as `:forward` to make the order of the query result in ascending by primary key(s), set it as `:backward` to make the order of the query result in descending by primary key(s).
        * `columns_to_get`, string list, return the specified attribute columns, if not specify this field all attribute columns will be return.
        * `start_column`, string, used as a starting column for Wide Column read, the return result contains this as starter.
        * `end_column`, string, used as a ending column for Wide Column read, the return result DON NOT contain this column.
        * `filter`, used as a filter by condition, support `">"`, `"<"`, `">="`, `"<="`, `"=="`, `"and"`, `"or"` and `"()"` expressions.
        
            The `ignore_if_missing` can be used for the non-existed attribute column, for example:

            An attribute column does not exist meanwhile set it as `true`, will ignore this match condition in the return result;

            An existed attribute column DOES NOT suit for this usecase, the match condition will always affect the return result, if match condition does not satisfy, they won't be
            return in result.

            ```elixir
            filter: filter(("name[ignore_if_missing: true]" == var_name and "age" > 1) or ("class" == "1"))
            ```

        * `transaction_id`, read under local transaction in a partition key.

      """
      @spec get_range(
              schema :: Ecto.Queryable.t(),
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
        Ecto.Adapters.Tablestore.get_range(
          get_dynamic_repo(),
          schema,
          start_primary_keys,
          end_primary_keys,
          options
        )
      end

      @doc """
      Batch get several rows of data from one or more tables, this batch request put multiple queries in one request from client's perspective.
      """
      @spec batch_get(gets) ::
              {:ok, Keyword.t()} | {:error, term()}
            when gets:
                   [
                     {
                       module :: Ecto.Schema.t(),
                       [{key :: String.t() | atom(), value :: integer | String.t()}],
                       options :: Keyword.t()
                     }
                   ]
                   | [
                       {
                         module :: Ecto.Schema.t(),
                         [{key :: String.t() | atom(), value :: integer | String.t()}]
                       }
                     ]
                   | [
                       [schema_entity :: Ecto.Schema.t()]
                     ]
                   | [
                       {[schema_entity :: Ecto.Schema.t()], options :: Keyword.t()}
                     ]
      def batch_get(gets) do
        Ecto.Adapters.Tablestore.batch_get(
          get_dynamic_repo(),
          gets
        )
      end

      def batch_write(writes) do
        Ecto.Adapters.Tablestore.batch_write(
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
        raise "Not set `instance` in Repo configuration properly, or found its value is not an :atom."
    end
  end

  ## Schema

  @impl true
  def autogenerate(:id), do: nil
  def autogenerate(:binary_id), do: nil

  @impl true
  def insert(repo, schema_meta, fields, on_conflict, returning, options) do
    IO.puts(
      "insert - repo: #{inspect(repo)}, schema_meta: #{inspect(schema_meta)}, fields: #{
        inspect(fields)
      }, on_conflict: #{inspect(on_conflict)}, returning: #{inspect(returning)}, options: #{
        inspect(options)
      }"
    )

    schema = schema_meta.schema

    {pks, attrs, autogenerate_id_name} = pks_and_attrs_to_put_row(schema, fields)

    result =
      Tablestore.execute_put_row(
        repo.instance,
        schema_meta.source,
        pks,
        attrs,
        options
      )

    IO.puts(">>> result:#{inspect(result)}")

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
    IO.puts(
      "delete - repo: #{inspect(repo)}, meta: #{inspect(schema_meta)}, filters: #{
        inspect(filters)
      }, opts: #{inspect(options)}"
    )

    result =
      Tablestore.execute_delete_row(
        repo.instance,
        schema_meta.source,
        format_key_to_str(filters),
        options
      )

    IO.puts(">>> delete_row result: #{inspect(result)}")

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
  def update(repo, schema_meta, fields, filters, returning, options) do
    IO.puts(
      "update - repo: #{inspect(repo)}, schema_meta: #{inspect(schema_meta)}, fields: #{
        inspect(fields)
      }, filters: #{inspect(filters)}, returning: #{inspect(returning)}, options: #{
        inspect(options)
      }"
    )

    result =
      Tablestore.execute_update_row(
        repo.instance,
        schema_meta.source,
        format_key_to_str(filters),
        Keyword.merge(options, map_attrs_to_update(fields))
      )

    IO.puts(">>> update_row result: #{inspect(result)}")

    case result do
      {:ok, response} ->
        case response.row do
          nil ->
            {:ok, []}

          _ ->
            {:ok, extract_as_keyword(response.row)}
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
      }, options: #{inspect(options)}"
    )
  end

  @doc false
  def get(repo, schema, ids, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    result =
      Tablestore.execute_get_row(
        meta.instance,
        schema.__schema__(:source),
        format_key_to_str(ids),
        options
      )

    case result do
      {:ok, response} ->
        row_to_schema(response.row, schema)

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
      Tablestore.execute_get_range(
        meta.instance,
        schema.__schema__(:source),
        prepared_start_primary_keys,
        format_key_to_str(end_primary_keys),
        options
      )

    IO.puts("get_range result: #{inspect(result)}")

    case result do
      {:ok, response} ->
        records =
          Enum.map(response.rows, fn row ->
            row_to_schema(row, schema)
          end)

        {records, response.next_start_primary_key}

      _error ->
        result
    end
  end

  def batch_get(repo, gets) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    {requests, schemas_mapping} = Enum.reduce(gets, {[], %{}}, &map_batch_gets/2)

    prepared_requests = Enum.reverse(requests)

    result = Tablestore.execute_batch_get(meta.instance, prepared_requests)

    case result do
      {:ok, response} ->
        batch_get_row_response_to_schemas(response.tables, schemas_mapping)

      _error ->
        result
    end
  end

  def batch_write(repo, writes) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    {var_write_requests, schema_entities_map} =
      writes
      |> Enum.map(&map_batch_writes/1)
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

    result = Tablestore.execute_batch_write(meta.instance, prepared_requests, [])

    case result do
      {:ok, response} ->
        batch_write_row_response_to_schemas(response.tables, input_schema_entities, operations)

      _error ->
        result
    end
  end

  def generate_condition_options(%{__meta__: _meta} = entity, options) do
    condition =
      entity
      |> generate_filter_options(:and, [])
      |> do_generate_condition(Keyword.get(options, :condition))

    Keyword.put(options, :condition, condition)
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

  def generate_filter_options(%{__meta__: _meta} = entity, :and, options) do
    attr_columns = entity_attr_columns(entity)

    options =
      attr_columns
      |> generate_filter_ast([])
      |> do_generate_filter_options(:and, options)

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

  defp do_generate_filter_options(nil, :and, options) do
    options
  end

  defp do_generate_filter_options(ast, :and, options) do
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

  defp pks_and_attrs_to_put_row(schema, fields) do
    primary_keys = schema.__schema__(:primary_key)
    autogenerate_id = schema.__schema__(:autogenerate_id)
    map_pks_and_attrs_to_put_row(primary_keys, autogenerate_id, fields, [])
  end

  defp map_pks_and_attrs_to_put_row(
         [],
         nil,
         attr_columns,
         prepared_pks
       ) do
    attrs =
      for {attr_key, attr_value} <- attr_columns do
        {Atom.to_string(attr_key), attr_value}
      end

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
         prepared_pks
       ) do
    attrs =
      for {attr_key, attr_value} <- attr_columns do
        {Atom.to_string(attr_key), attr_value}
      end

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
         prepared_pks
       ) do
    {value, updated_fields} = Keyword.pop(fields, primary_key)

    if value == nil,
      do: raise("Invalid usecase - primary key: `#{primary_key}` can not be nil.")

    update = [{Atom.to_string(primary_key), value} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, nil, updated_fields, update)
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks
       )
       when primary_key == autogenerate_id_name do
    update = [{Atom.to_string(autogenerate_id_name), PKType.auto_increment()} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, autogenerate_id, fields, update)
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks
       )
       when primary_key != autogenerate_id_name do
    {value, updated_fields} = Keyword.pop(fields, primary_key)

    if value == nil,
      do: raise("Invalid usecase - primary key: `#{primary_key}` can not be nil.")

    update = [{Atom.to_string(primary_key), value} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, autogenerate_id, updated_fields, update)
  end

  defp map_pks_and_attrs_to_put_row(
         _,
         {_, _autogenerate_id_name, :binary_id},
         _fields,
         _prepared_pks
       ) do
    raise "Not support autogenerate id as string (`:binary_id`)."
  end

  defp row_to_schema(nil, _schema) do
    nil
  end

  defp row_to_schema(row, schema) do
    struct(schema, extract_as_keyword(row))
  end

  defp extract_as_keyword({nil, attrs}) do
    for {attr_key, attr_value, _ts} <- attrs do
      {String.to_atom(attr_key), attr_value}
    end
  end

  defp extract_as_keyword({pks, nil}) do
    for {pk_key, pk_value} <- pks do
      {String.to_atom(pk_key), pk_value}
    end
  end

  defp extract_as_keyword({pks, attrs}) do
    prepared_pks =
      for {pk_key, pk_value} <- pks do
        {String.to_atom(pk_key), pk_value}
      end

    prepared_attrs =
      for {attr_key, attr_value, _ts} <- attrs do
        {String.to_atom(attr_key), attr_value}
      end

    IO.puts("extract_as_keyword prepared_attrs: #{inspect(prepared_attrs)}")
    Keyword.merge(prepared_attrs, prepared_pks)
  end

  defp map_attrs_to_update(attrs) do
    Enum.reduce(attrs, Keyword.new(), &construct_row_updates/2)
  end

  defp construct_row_updates({key, nil}, acc) when is_atom(key) do
    if Keyword.has_key?(acc, :delete_all) do
      Keyword.update!(acc, :delete_all, &[Atom.to_string(key) | &1])
    else
      Keyword.put(acc, :delete_all, [Atom.to_string(key)])
    end
  end

  defp construct_row_updates({key, {:increment, value}}, acc) when is_integer(value) do
    key_str = Atom.to_string(key)

    if Keyword.has_key?(acc, :increment) do
      acc
      |> Keyword.update!(:increment, &[{key_str, value} | &1])
      |> Keyword.update!(:return_columns, &[key_str | &1])
    else
      acc
      |> Keyword.put(:increment, [{key_str, value}])
      |> Keyword.put(:return_type, ReturnType.after_modify())
      |> Keyword.put(:return_columns, [key_str])
    end
  end

  defp construct_row_updates({key, value}, acc) when is_atom(key) do
    case Keyword.has_key?(acc, :put) do
      false ->
        Keyword.put(acc, :put, [{Atom.to_string(key), value}])

      true ->
        Keyword.update!(acc, :put, &[{Atom.to_string(key), value} | &1])
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
        opts = generate_filter_options(schema_entity, :and, options)

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
      Tablestore.execute_get(
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
      Tablestore.execute_get(
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

  defp map_batch_writes({:delete, deletes}) do
    Enum.reduce(deletes, {%{}, %{}}, fn delete, {delete_acc, schema_entities_acc} ->
      {source, ids, options, schema_entity} = do_map_batch_writes(:delete, delete)
      write_delete_request = Tablestore.execute_write_delete(ids, options)

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

  defp map_batch_writes({:put, puts}) do
    Enum.reduce(puts, {%{}, %{}}, fn put, {puts_acc, schema_entities_acc} ->
      {source, ids, attrs, options, schema_entity} = do_map_batch_writes(:put, put)
      write_put_request = Tablestore.execute_write_put(ids, attrs, options)

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

  defp map_batch_writes({:update, updates}) do
    Enum.reduce(updates, {%{}, %{}}, fn update, {update_acc, schema_entities_acc} ->
      {source, ids, options, schema_entity} = do_map_batch_writes(:update, update)
      write_update_request = Tablestore.execute_write_update(ids, options)

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

  defp map_batch_writes(item) do
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

  defp do_map_batch_writes(:put, %{__meta__: _meta} = schema_entity) do
    do_map_batch_writes(:put, {schema_entity, []})
  end

  defp do_map_batch_writes(:put, {%{__meta__: meta} = schema_entity, options}) do
    schema = meta.schema
    source = schema.__schema__(:source)

    fields =
      schema_entity
      |> Ecto.primary_key()
      |> Enum.reduce(entity_attr_columns(schema_entity), fn {key, value}, acc ->
        if value != nil, do: [{key, value} | acc], else: acc
      end)

    {pks, attrs, _autogenerate_id_name} = pks_and_attrs_to_put_row(schema, fields)
    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(:put, {schema, ids, attrs, options}) do
    source = schema.__schema__(:source)
    fields = ids ++ attrs
    schema_entity = struct(schema, fields)
    {pks, attrs, _autogenerate_id_name} = pks_and_attrs_to_put_row(schema, fields)
    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(
         :update,
         {%Ecto.Changeset{valid?: true, data: %{__meta__: meta}} = changeset, options}
       ) do
    IO.puts(
      ">>> changes: #{inspect(changeset.changes)}, meta: #{inspect(meta)}, schema: #{
        inspect(meta.schema)
      }<<<"
    )

    source = meta.schema.__schema__(:source)
    entity = changeset.data
    ids = Ecto.primary_key(entity)
    options = generate_condition_options(entity, options)
    changes = changeset.changes
    update_attrs = map_attrs_to_update(changes)

    schema = meta.schema
    fields = ids ++ Map.to_list(changes)
    IO.puts("schema: #{inspect(schema)}, fields: #{inspect(fields)}")
    schema_entity = struct(schema, fields)

    IO.puts(">>> update changes, schema_entity: #{inspect(schema_entity)}")

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
          schema_data = row_to_schema(row_in_batch.row, schema)
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
          schema_entity_from_response = row_to_schema(write_row_response.row, schema)

          return_schema_entity =
            if schema_entity_from_response != nil do
              Map.merge(input, schema_entity_from_response, fn _k, v1, v2 ->
                if v2 != nil, do: v2, else: v1
              end)
            else
              input
            end

          if Keyword.has_key?(acc, operation) do
            Keyword.update!(acc, operation, &[return_schema_entity | &1])
          else
            Keyword.put(acc, operation, [return_schema_entity])
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
end

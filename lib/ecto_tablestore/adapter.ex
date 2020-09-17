defmodule Ecto.Adapters.Tablestore do
  @moduledoc false
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema

  alias __MODULE__

  alias EctoTablestore.Sequence
  alias ExAliyunOts.Error

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

  @ots_condition_check_fail "OTSConditionCheckFail"

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      ## Query

      @spec search(schema :: Ecto.Schema.t(), index_name :: String.t(), options :: Keyword.t()) ::
              {:ok, EctoTablestore.Repo.search_result()} | {:error, term()}
      def search(schema, index_name, options) do
        Tablestore.search(get_dynamic_repo(), schema, index_name, options)
      end

      @spec one(entity :: Ecto.Schema.t(), options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil
      def one(%{__meta__: meta} = entity, options \\ []) do
        options =
          if Keyword.get(options, :entity_full_match, false) do
            Tablestore.generate_filter_options(entity, options)
          else
            options
          end

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
            ) :: {nil, nil} | {list, nil} | {list, binary()} | {:error, term()}
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

      @spec stream_range(
              schema :: Ecto.Schema.t(),
              start_primary_keys :: list,
              end_primary_keys :: list,
              options :: Keyword.t()
            ) :: Enumerable.t()
      def stream_range(
            schema,
            start_primary_keys,
            end_primary_keys,
            options \\ [direction: :forward]
          ) do
        Tablestore.stream_range(
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

      @spec batch_write(writes, options :: Keyword.t()) ::
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
      def batch_write(writes, options \\ []) do
        Tablestore.batch_write(
          get_dynamic_repo(),
          writes,
          options
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

    instance = repo.instance

    {pks, attrs, autogenerate_id_name} = pks_and_attrs_to_put_row(instance, schema, fields)

    result =
      ExAliyunOts.put_row(
        instance,
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
        {:invalid, [{:check, error.code}]}
    end
  end

  @impl true
  def delete(repo, schema_meta, filters, options) do
    result =
      ExAliyunOts.delete_row(
        repo.instance,
        schema_meta.source,
        prepare_primary_keys_by_order(schema_meta.schema, filters),
        Keyword.take(options, [:condition, :transaction_id])
      )

    case result do
      {:ok, _response} ->
        {:ok, []}

      {:error, error} ->
        case error do
          %Error{code: code} when code == @ots_condition_check_fail ->
            {:error, :stale}

          _ ->
            {:invalid, [{:check, error.code}]}
        end
    end
  end

  @impl true
  def update(repo, schema_meta, fields, filters, _returning, options) do
    schema = schema_meta.schema

    options =
      options
      |> Keyword.take([:condition, :transaction_id])
      |> Keyword.merge(map_attrs_to_update(schema, fields))

    result =
      ExAliyunOts.update_row(
        repo.instance,
        schema_meta.source,
        prepare_primary_keys_by_order(schema, filters),
        options
      )

    case result do
      {:ok, response} ->
        case response.row do
          nil ->
            {:ok, []}

          _ ->
            {:ok, extract_as_keyword(schema, response.row)}
        end

      {:error, error} ->
        case error do
          %Error{code: code} when code == @ots_condition_check_fail ->
            {:error, :stale}

          _ ->
            {:invalid, [{:check, error.code}]}
        end
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
      ExAliyunOts.search(
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
            aggs: response.aggs,
            group_bys: response.group_bys,
            schemas:
              Enum.map(response.rows, fn row ->
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
      ExAliyunOts.get_row(
        meta.instance,
        schema.__schema__(:source),
        prepare_primary_keys_by_order(schema, ids),
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

    result =
      ExAliyunOts.get_range(
        meta.instance,
        schema.__schema__(:source),
        prepare_start_primary_keys_by_order(schema, start_primary_keys),
        prepare_primary_keys_by_order(schema, end_primary_keys),
        options
      )

    case result do
      {:ok, response} ->
        {
          transfer_rows_by_schema(response.rows, schema),
          response.next_start_primary_key
        }

      _error ->
        result
    end
  end

  @doc false
  def stream_range(repo, schema, start_primary_keys, end_primary_keys, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    meta.instance
    |> ExAliyunOts.stream_range(
      schema.__schema__(:source),
      prepare_start_primary_keys_by_order(schema, start_primary_keys),
      prepare_primary_keys_by_order(schema, end_primary_keys),
      options
    )
    |> Stream.flat_map(fn
      {:ok, response} ->
        transfer_rows_by_schema(response.rows, schema, [])

      error ->
        [error]
    end)
  end

  defp transfer_rows_by_schema(rows, schema, default \\ nil)
  defp transfer_rows_by_schema(nil, _schema, default), do: default

  defp transfer_rows_by_schema(rows, schema, _default) when is_list(rows) do
    Enum.map(rows, fn row ->
      row_to_schema(schema, row)
    end)
  end

  @doc false
  def batch_get(repo, gets) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    {requests, schemas_mapping} = Enum.reduce(gets, {[], %{}}, &map_batch_gets/2)

    prepared_requests = Enum.reverse(requests)

    result = ExAliyunOts.batch_get(meta.instance, prepared_requests)

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
  def batch_write(repo, writes, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    instance = meta.instance

    {var_write_requests, schema_entities_map} =
      writes
      |> Enum.map(&map_batch_writes(instance, &1))
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

    result = ExAliyunOts.batch_write(instance, prepared_requests, options)

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
      |> generate_filter_from_entity([])
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
        implicit_columns_to_get
        |> splice_list(columns_to_get_opt)
        |> MapSet.new()
        |> MapSet.to_list()

      Keyword.put(options, :columns_to_get, updated_columns_to_get)
    end
  end

  defp generate_filter_from_entity([], []) do
    nil
  end

  defp generate_filter_from_entity([], [
         %ExAliyunOts.Var.Filter{filter_type: :FT_SINGLE_COLUMN_VALUE} = filter
       ]) do
    filter
  end

  defp generate_filter_from_entity([], prepared) do
    %ExAliyunOts.Var.Filter{
      filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
        combinator: :LO_AND,
        sub_filters: prepared
      },
      filter_type: :FT_COMPOSITE_COLUMN_VALUE
    }
  end

  defp generate_filter_from_entity([{field_name, value} | rest], prepared)
       when is_map(value) or is_list(value) do
    filter = %ExAliyunOts.Var.Filter{
      filter: %ExAliyunOts.Var.SingleColumnValueFilter{
        column_name: Atom.to_string(field_name),
        column_value: Jason.encode!(value),
        comparator: :CT_EQUAL,
        ignore_if_missing: false,
        latest_version_only: true
      },
      filter_type: :FT_SINGLE_COLUMN_VALUE
    }

    generate_filter_from_entity(rest, [filter | prepared])
  end

  defp generate_filter_from_entity([{field_name, value} | rest], prepared) do
    filter = %ExAliyunOts.Var.Filter{
      filter: %ExAliyunOts.Var.SingleColumnValueFilter{
        column_name: Atom.to_string(field_name),
        column_value: value,
        comparator: :CT_EQUAL,
        ignore_if_missing: false,
        latest_version_only: true
      },
      filter_type: :FT_SINGLE_COLUMN_VALUE
    }

    generate_filter_from_entity(rest, [filter | prepared])
  end

  @doc false
  def execute_ddl(repo, definition) do
    repo
    |> Ecto.Adapter.lookup_meta()
    |> do_execute_ddl(definition)
  end

  @doc false
  def key_to_global_sequence(table_name, field) do
    "#{table_name},#{field}"
  end

  @doc false
  def bound_sequence_table_name(table_name) do
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

  defp do_generate_filter_options(filter_from_entity, options) do
    merged = do_generate_filter(filter_from_entity, :and, options[:filter])
    Keyword.put(options, :filter, merged)
  end

  defp do_generate_filter(filter_from_entity, :and, nil) do
    filter_from_entity
  end

  defp do_generate_filter(filter_from_entity, :and, %ExAliyunOts.Var.Filter{} = filter_from_opt) do
    filter_names_from_opt = do_generate_filter_iterate(filter_from_opt)

    filter_from_entity = do_drop_filter_from_entity(filter_from_entity, filter_names_from_opt)

    case filter_from_entity do
      filter_from_entity when is_list(filter_from_entity) ->
        %ExAliyunOts.Var.Filter{
          filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
            combinator: LogicOperator.and(),
            sub_filters: Enum.reverse([filter_from_opt | filter_from_entity])
          },
          filter_type: FilterType.composite_column()
        }

      %ExAliyunOts.Var.Filter{filter_type: :FT_SINGLE_COLUMN_VALUE} ->
        %ExAliyunOts.Var.Filter{
          filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
            combinator: LogicOperator.and(),
            sub_filters: [filter_from_entity, filter_from_opt]
          },
          filter_type: FilterType.composite_column()
        }

      nil ->
        filter_from_opt
    end
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

  defp do_generate_filter_iterate(%ExAliyunOts.Var.Filter{
         filter: %{sub_filters: sub_filters},
         filter_type: :FT_COMPOSITE_COLUMN_VALUE
       }) do
    sub_filters
    |> Enum.map(fn filter ->
      do_generate_filter_iterate(filter)
    end)
    |> List.flatten()
  end

  defp do_generate_filter_iterate(%ExAliyunOts.Var.Filter{
         filter: %{column_name: column_name},
         filter_type: :FT_SINGLE_COLUMN_VALUE
       }) do
    [column_name]
  end

  defp do_drop_filter_from_entity(
         %ExAliyunOts.Var.Filter{
           filter: %{column_name: column_name},
           filter_type: :FT_SINGLE_COLUMN_VALUE
         } = filter,
         fields
       ) do
    if column_name in fields, do: nil, else: filter
  end

  defp do_drop_filter_from_entity(
         %ExAliyunOts.Var.Filter{
           filter: %{sub_filters: sub_filters},
           filter_type: :FT_COMPOSITE_COLUMN_VALUE
         },
         fields
       ) do
    filter_from_entity =
      sub_filters
      |> Enum.reduce([], fn filter, acc ->
        f = do_drop_filter_from_entity(filter, fields)
        if f != nil, do: [f | acc], else: acc
      end)
      |> Enum.reverse()

    if filter_from_entity == [], do: nil, else: filter_from_entity
  end

  defp pks_and_attrs_to_put_row(instance, schema, fields) do
    primary_keys = schema.__schema__(:primary_key)
    autogenerate_id = schema.__schema__(:autogenerate_id)
    map_pks_and_attrs_to_put_row(primary_keys, autogenerate_id, fields, [], instance, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         [],
         nil,
         attr_columns,
         prepared_pks,
         _instance,
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
         {_, autogenerate_id_name, EctoTablestore.Hashids},
         attr_columns,
         prepared_pks,
         _instance,
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
         [],
         {_, autogenerate_id_name, :id},
         attr_columns,
         prepared_pks,
         _instance,
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
         instance,
         schema
       ) do
    {value, updated_fields} = Keyword.pop(fields, primary_key)

    if value == nil,
      do: raise("Invalid usecase - primary key: `#{primary_key}` can not be nil.")

    update = [{Atom.to_string(primary_key), value} | prepared_pks]
    map_pks_and_attrs_to_put_row(rest_primary_keys, nil, updated_fields, update, instance, schema)
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, EctoTablestore.Hashids} = autogenerate_id,
         fields,
         prepared_pks,
         instance,
         schema
       )
       when primary_key == autogenerate_id_name and is_list(prepared_pks) do
    source = schema.__schema__(:source)

    field_name_str = Atom.to_string(autogenerate_id_name)

    next_value = Sequence.next_value(instance, key_to_global_sequence(source, field_name_str))

    hashids_value = Hashids.encode(schema.hashids(primary_key), next_value)

    prepared_pks = [{field_name_str, hashids_value} | prepared_pks]

    map_pks_and_attrs_to_put_row(
      rest_primary_keys,
      autogenerate_id,
      fields,
      prepared_pks,
      instance,
      schema
    )
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, EctoTablestore.Hashids} = autogenerate_id,
         fields,
         prepared_pks,
         instance,
         schema
       )
       when primary_key != autogenerate_id_name do
    {value, updated_fields} = Keyword.pop(fields, primary_key)

    if value == nil,
      do: raise("Invalid usecase - autogenerate primary key: `#{primary_key}` can not be nil.")

    prepared_pks = [{Atom.to_string(primary_key), value} | prepared_pks]

    map_pks_and_attrs_to_put_row(
      rest_primary_keys,
      autogenerate_id,
      updated_fields,
      prepared_pks,
      instance,
      schema
    )
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         [],
         instance,
         schema
       )
       when primary_key == autogenerate_id_name do
    # Set partition_key as auto-generated, use sequence for this usecase

    source = schema.__schema__(:source)

    field_name_str = Atom.to_string(autogenerate_id_name)

    next_value = Sequence.next_value(instance, bound_sequence_table_name(source), field_name_str)

    prepared_pks = [{field_name_str, next_value}]

    map_pks_and_attrs_to_put_row(
      rest_primary_keys,
      autogenerate_id,
      fields,
      prepared_pks,
      instance,
      schema
    )
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks,
         instance,
         schema
       )
       when primary_key == autogenerate_id_name do
    update = [{Atom.to_string(autogenerate_id_name), PKType.auto_increment()} | prepared_pks]

    map_pks_and_attrs_to_put_row(
      rest_primary_keys,
      autogenerate_id,
      fields,
      update,
      instance,
      schema
    )
  end

  defp map_pks_and_attrs_to_put_row(
         [primary_key | rest_primary_keys],
         {_, autogenerate_id_name, :id} = autogenerate_id,
         fields,
         prepared_pks,
         instance,
         schema
       )
       when primary_key != autogenerate_id_name do
    {value, updated_fields} = Keyword.pop(fields, primary_key)

    if value == nil,
      do: raise("Invalid usecase - autogenerate primary key: `#{primary_key}` can not be nil.")

    prepared_pks = [{Atom.to_string(primary_key), value} | prepared_pks]

    map_pks_and_attrs_to_put_row(
      rest_primary_keys,
      autogenerate_id,
      updated_fields,
      prepared_pks,
      instance,
      schema
    )
  end

  defp map_pks_and_attrs_to_put_row(
         _,
         {_, _autogenerate_id_name, :binary_id},
         _fields,
         _prepared_pks,
         _instance,
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
       when type in [:naive_datetime_usec, :naive_datetime] and is_atom(key) do
    {key, value |> DateTime.from_unix!() |> DateTime.to_naive()}
  end

  defp do_map_row_item_to_attr(type, key, value)
       when type in [:utc_datetime, :utc_datetime_usec] and is_atom(key) do
    {key, DateTime.from_unix!(value)}
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

  defp construct_row_updates({field, {:increment, value}}, {schema, acc})
       when is_integer(value) do
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

  defp prepare_primary_keys_by_order(schema_entity) do
    schema_entity
    |> Ecto.primary_key()
    |> Enum.map(fn {key, value} ->
      {Atom.to_string(key), map_key_value(value)}
    end)
  end

  defp prepare_start_primary_keys_by_order(schema, start_primary_keys) do
    cond do
      is_list(start_primary_keys) ->
        prepare_primary_keys_by_order(schema, start_primary_keys)

      is_binary(start_primary_keys) ->
        start_primary_keys

      true ->
        raise "Invalid start_primary_keys: #{inspect(start_primary_keys)}, expect it as `list` or `binary`"
    end
  end

  defp prepare_primary_keys_by_order(schema, input_primary_keys)
       when is_list(input_primary_keys) do
    struct(schema)
    |> Ecto.primary_key()
    |> Enum.map(fn {key, _} ->
      key_str = Atom.to_string(key)

      value =
        input_primary_keys
        |> Enum.find_value(fn {input_k, input_v} ->
          cond do
            is_atom(input_k) and input_k == key ->
              input_v

            is_bitstring(input_k) and input_k == key_str ->
              input_v

            true ->
              nil
          end
        end)
        |> map_key_value()

      {key_str, value}
    end)
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
            prepare_primary_keys_by_order(schema_entity),
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
        opts =
          if Keyword.get(options, :entity_full_match, false) do
            generate_filter_options(schema_entity, options)
          else
            options
          end

        prepared_columns_to_get =
          opts
          |> Keyword.get(:columns_to_get, [])
          |> splice_list(columns_to_get)
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
      ExAliyunOts.get(
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
      ExAliyunOts.get(
        source,
        format_ids_groups(schema, ids_groups),
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

  defp map_batch_writes(_instance, {:delete, deletes}) do
    Enum.reduce(deletes, {%{}, %{}}, fn delete, {delete_acc, schema_entities_acc} ->
      {source, ids, options, schema_entity} = do_map_batch_writes(:delete, delete)
      write_delete_request = ExAliyunOts.write_delete(ids, options)

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

  defp map_batch_writes(instance, {:put, puts}) do
    Enum.reduce(puts, {%{}, %{}}, fn put, {puts_acc, schema_entities_acc} ->
      {source, ids, attrs, options, schema_entity} = do_map_batch_writes(:put, {instance, put})

      write_put_request = ExAliyunOts.write_put(ids, attrs, options)

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

  defp map_batch_writes(_instance, {:update, updates}) do
    Enum.reduce(updates, {%{}, %{}}, fn update, {update_acc, schema_entities_acc} ->
      {source, ids, options, schema_entity} = do_map_batch_writes(:update, update)

      write_update_request = ExAliyunOts.write_update(ids, options)

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

  defp map_batch_writes(_instance, item) do
    raise("Invalid usecase - batch write with item: #{inspect(item)}")
  end

  defp do_map_batch_writes(:delete, %{__meta__: _meta} = schema_entity) do
    do_map_batch_writes(:delete, {schema_entity, []})
  end

  defp do_map_batch_writes(:delete, {%{__meta__: meta} = schema_entity, options}) do
    source = meta.schema.__schema__(:source)
    ids = prepare_primary_keys_by_order(schema_entity)
    options = generate_condition_options(schema_entity, options)
    {source, ids, options, schema_entity}
  end

  defp do_map_batch_writes(:delete, {schema, ids, options}) do
    source = schema.__schema__(:source)
    schema_entity = struct(schema, ids)
    ids = prepare_primary_keys_by_order(schema_entity)
    {source, ids, options, schema_entity}
  end

  defp do_map_batch_writes(:put, {instance, %{__meta__: _meta} = schema_entity}) do
    do_map_batch_writes(:put, {instance, {schema_entity, []}})
  end

  defp do_map_batch_writes(:put, {instance, {%{__meta__: meta} = schema_entity, options}}) do
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
      Enum.reduce(autogen_fields, schema_entity, fn {key, value}, acc ->
        Map.put(acc, key, value)
      end)

    {pks, attrs, _autogenerate_id_name} =
      pks_and_attrs_to_put_row(instance, schema, Keyword.merge(autogen_fields, fields))

    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(:put, {instance, {schema, ids, attrs, options}}) do
    source = schema.__schema__(:source)
    autogen_fields = autogen_fields(schema)
    fields = Keyword.merge(autogen_fields, splice_list(ids, attrs))
    schema_entity = struct(schema, fields)

    {pks, attrs, _autogenerate_id_name} = pks_and_attrs_to_put_row(instance, schema, fields)
    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(
         :put,
         {instance, {%Ecto.Changeset{valid?: true, data: %{__meta__: meta}} = changeset, options}}
       ) do
    schema = meta.schema
    source = schema.__schema__(:source)
    autogen_fields = autogen_fields(schema)

    input_fields =
      changeset.data
      |> Ecto.primary_key()
      |> Enum.reduce(Map.to_list(changeset.changes), fn {key, value}, acc ->
        if value != nil, do: [{key, value} | acc], else: acc
      end)

    fields = Keyword.merge(autogen_fields, input_fields)

    schema_entity = struct(schema, fields)

    {pks, attrs, _autogenerate_id_name} = pks_and_attrs_to_put_row(instance, schema, fields)
    {source, pks, attrs, options, schema_entity}
  end

  defp do_map_batch_writes(
         :put,
         {instance, %Ecto.Changeset{valid?: true} = changeset}
       ) do
    do_map_batch_writes(:put, {instance, {changeset, []}})
  end

  defp do_map_batch_writes(
         :put,
         {_instance, {%Ecto.Changeset{valid?: false} = changeset, _options}}
       ) do
    raise "Using invalid changeset: #{inspect(changeset)} in batch writes"
  end

  defp do_map_batch_writes(
         :put,
         {_instance, %Ecto.Changeset{valid?: false} = changeset}
       ) do
    raise "Using invalid changeset: #{inspect(changeset)} in batch writes"
  end

  defp do_map_batch_writes(
         :update,
         {%Ecto.Changeset{valid?: true, data: %{__meta__: meta}} = changeset, options}
       ) do
    schema = meta.schema
    source = schema.__schema__(:source)
    entity = changeset.data

    options = generate_condition_options(entity, options)

    autoupdate_fields = autoupdate_fields(schema)

    changes =
      Enum.reduce(autoupdate_fields, changeset.changes, fn {key, value}, acc ->
        Map.put(acc, key, value)
      end)

    update_attrs = map_attrs_to_update(schema, changes)

    schema_entity = do_map_merge(changeset.data, changes, true)

    {source, prepare_primary_keys_by_order(entity), merge_options(options, update_attrs),
     schema_entity}
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

  defp do_map_batch_writes(:update, {%Ecto.Changeset{valid?: false} = changeset, _options}) do
    raise "Using invalid changeset: #{inspect(changeset)} in batch writes"
  end

  defp format_ids_groups(schema, [ids] = ids_groups) when is_list(ids) do
    ids_groups
    |> Enum.map(fn ids_group ->
      if is_list(ids_group),
        do: prepare_primary_keys_by_order(schema, ids_group),
        else: prepare_primary_keys_by_order(schema, [ids_group])
    end)
  end

  defp format_ids_groups(schema, ids_groups) when is_list(ids_groups) do
    primary_keys_size = length(schema.__schema__(:primary_key))

    if primary_keys_size == length(ids_groups) do
      # fetch a single row with multi primary_key(s)
      format_ids_groups(schema, [ids_groups])
    else
      # fetch multi rows with multi primary_keys
      Enum.map(ids_groups, fn ids_group ->
        prepare_primary_keys_by_order(schema, ids_group)
      end)
    end
  end

  defp autogen_fields(schema) do
    case schema.__schema__(:autogenerate) do
      [{autogen_fields, {m, f, a}}] ->
        autogen_value = apply(m, f, a)
        Enum.map(autogen_fields, fn f -> {f, autogen_value} end)

      _ ->
        []
    end
  end

  defp autoupdate_fields(schema) do
    case schema.__schema__(:autoupdate) do
      [{autoupdate_fields, {m, f, a}}] ->
        autoupdate_value = apply(m, f, a)
        Enum.map(autoupdate_fields, fn f -> {f, autoupdate_value} end)

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

  defp do_merge_option(input, generated) when is_list(input) and is_list(generated) do
    input |> splice_list(generated) |> MapSet.new() |> MapSet.to_list()
  end

  defp do_merge_option(input, generated) when input == generated, do: input
  defp do_merge_option(_input, generated), do: generated

  defp batch_get_row_response_to_schemas(tables, schemas_mapping) do
    tables
    |> Enum.reduce([], fn table, acc ->
      schema = Map.get(schemas_mapping, table.table_name)

      schemas_data =
        Enum.reduce(table.rows, [], fn row_in_batch, acc ->
          schema_data = row_to_schema(schema, row_in_batch.row)
          if schema_data == nil, do: acc, else: [schema_data | acc]
        end)

      case schemas_data do
        [] ->
          [{schema, nil} | acc]

        _ ->
          [{schema, Enum.reverse(schemas_data)} | acc]
      end
    end)
    |> Enum.reverse()
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
        splice_list(v1, v2)
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
    Enum.reduce(keyword, map, fn {key, value}, acc ->
      Map.put(acc, key, value)
    end)
  end

  defp do_map_merge(map, keyword, false) when is_map(map) and is_list(keyword) do
    Enum.reduce(keyword, map, fn {key, value}, acc ->
      if value != nil, do: Map.put(acc, key, value), else: acc
    end)
  end

  ## Migration

  defp do_execute_ddl(meta, {:create, table, columns}) do
    {table_name, primary_keys, create_seq?, with_hashids?} =
      Enum.reduce(columns, {table.name, [], false, false}, &do_execute_ddl_add_column/2)

    Logger.info(fn ->
      ">> table_name: #{table_name}, primary_keys: #{inspect(primary_keys)}, create_seq?: #{
        create_seq?
      }"
    end)

    instance = meta.instance

    result =
      ExAliyunOts.create_table(
        instance,
        table_name,
        primary_keys,
        Keyword.put(table.meta, :max_versions, 1)
      )

    Logger.info(fn ->
      "create a table: #{table_name} result: #{inspect(result)}"
    end)

    if result == :ok and create_seq? do
      if with_hashids? do
        # so far only process `hashids` type in migration with
        # the global default table `ecto_tablestore_default_seq`,
        # and it will be applied and recommended in the future by default.
        Sequence.create(instance)
      else
        Sequence.create(instance, bound_sequence_table_name(table_name))
      end
    end
  end

  defp do_execute_ddl_add_column(
         {:add, field_name, :hashids, options},
         {source, prepared_columns, create_seq?, _with_hashids}
       )
       when is_atom(field_name) do
    partition_key? = Keyword.get(options, :partition_key, false)
    auto_increment? = Keyword.get(options, :auto_increment, false)

    {prepared_column, create_seq?} =
      cond do
        partition_key? and auto_increment? ->
          {
            {Atom.to_string(field_name), PKType.string()},
            true
          }

        create_seq? and auto_increment? ->
          raise Ecto.MigrationError,
            message:
              "the `auto_increment: true` option only allows binding of one primary key, but find the duplicated `#{
                field_name
              }` field with options: #{inspect(options)}"

        true ->
          {
            {Atom.to_string(field_name), PKType.string()},
            true
          }
      end

    {
      source,
      splice_list(prepared_columns, [prepared_column]),
      create_seq?,
      true
    }
  end

  defp do_execute_ddl_add_column(
         {:add, field_name, :integer, options},
         {source, prepared_columns, create_seq?, with_hashids?}
       )
       when is_atom(field_name) do
    partition_key? = Keyword.get(options, :partition_key, false)
    auto_increment? = Keyword.get(options, :auto_increment, false)

    {prepared_column, create_seq?} =
      cond do
        partition_key? == true and auto_increment? == true ->
          {
            {Atom.to_string(field_name), PKType.integer()},
            true
          }

        create_seq? and auto_increment? == true ->
          raise Ecto.MigrationError,
            message:
              "the `auto_increment: true` option only allows binding of one primary key, but find the duplicated `#{
                field_name
              }` field with options: #{inspect(options)}"

        auto_increment? == true ->
          {
            {Atom.to_string(field_name), PKType.integer(), PKType.auto_increment()},
            create_seq?
          }

        true ->
          {
            {Atom.to_string(field_name), PKType.integer()},
            create_seq?
          }
      end

    {
      source,
      splice_list(prepared_columns, [prepared_column]),
      create_seq?,
      with_hashids?
    }
  end

  defp do_execute_ddl_add_column(
         {:add, field_name, :string, _options},
         {source, prepared_columns, create_seq?, with_hashids?}
       )
       when is_atom(field_name) do
    {
      source,
      splice_list(prepared_columns, [{Atom.to_string(field_name), PKType.string()}]),
      create_seq?,
      with_hashids?
    }
  end

  defp do_execute_ddl_add_column(
         {:add, field_name, :binary, _options},
         {source, prepared_columns, create_seq?, with_hashids?}
       )
       when is_atom(field_name) do
    {
      source,
      splice_list(prepared_columns, [{Atom.to_string(field_name), PKType.binary()}]),
      create_seq?,
      with_hashids?
    }
  end

  defp splice_list(list1, list2) when is_list(list1) and is_list(list2) do
    List.flatten([list1 | list2])
  end
end

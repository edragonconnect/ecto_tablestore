defmodule Ecto.Adapters.Tablestore do
  @moduledoc false
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Storage

  alias __MODULE__

  alias EctoTablestore.Sequence
  alias ExAliyunOts.Error
  alias ExAliyunOts.TableStore.Condition
  alias ExAliyunOts.TableStoreFilter.{Filter, CompositeColumnValueFilter, SingleColumnValueFilter}

  alias ExAliyunOts.Const.{
    PKType,
    ReturnType,
    FilterType,
    LogicOperator,
    RowExistence,
    OperationType,
    ComparatorType
  }

  require PKType
  require ReturnType
  require FilterType
  require LogicOperator
  require RowExistence
  require OperationType
  require ComparatorType

  require Logger

  @type options :: Keyword.t()
  @type start_primary_keys :: list | binary
  @type end_primary_keys :: list
  @type batch_gets :: [
          {
            module :: Ecto.Schema.t(),
            [
              [{key :: String.t() | atom, value :: integer | String.t()}]
            ]
          }
          | {
              module :: Ecto.Schema.t(),
              [
                [{key :: String.t() | atom, value :: integer | String.t()}]
              ],
              options :: Keyword.t()
            }
          | {
              module :: Ecto.Schema.t(),
              [{key :: String.t() | atom, value :: integer | String.t()}]
            }
          | {
              module :: Ecto.Schema.t(),
              [{key :: String.t() | atom, value :: integer | String.t()}],
              options :: Keyword.t()
            }
          | [schema_entity :: Ecto.Schema.t()]
          | {[schema_entity :: Ecto.Schema.t()], options :: Keyword.t()}
        ]
  @type batch_writes :: [
          {
            operation :: :put,
            items :: [
              schema_entity ::
                Ecto.Schema.t()
                | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
                | {module :: Ecto.Schema.t(), ids :: list, attrs :: list, options :: Keyword.t()}
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
                  | {module :: Ecto.Schema.t(), ids :: list, options :: Keyword.t()}
              ]
            }
        ]

  @ots_condition_check_fail "OTSConditionCheckFail"

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      ## Query

      @spec search(Ecto.Schema.t(), ExAliyunOts.index_name(), Tablestore.options()) ::
              {:ok, EctoTablestore.Repo.search_result()} | {:error, term}
      def search(schema, index_name, options) do
        Tablestore.search(get_dynamic_repo(), schema, index_name, options)
      end

      @spec stream_search(Ecto.Schema.t(), ExAliyunOts.index_name(), Tablestore.options()) ::
              Enumerable.t()
      def stream_search(schema, index_name, options) do
        Tablestore.stream_search(get_dynamic_repo(), schema, index_name, options)
      end

      @spec one(Ecto.Schema.t(), Tablestore.options()) ::
              Ecto.Schema.t() | {:error, term} | nil
      def one(%{__meta__: meta} = entity, options \\ []) do
        options = Tablestore.generate_filter_options(entity, options)
        get(meta.schema, Ecto.primary_key(entity), options)
      end

      @spec get(Ecto.Schema.t(), ids :: list, Tablestore.options()) ::
              Ecto.Schema.t() | {:error, term} | nil
      def get(schema, ids, options \\ []) do
        Tablestore.get(get_dynamic_repo(), schema, ids, options)
      end

      @spec get_range(Ecto.Schema.t(), Tablestore.options()) ::
              {nil, nil} | {list, nil} | {list, binary} | {:error, term}
      def get_range(schema, options \\ [direction: :forward]) do
        Tablestore.get_range(get_dynamic_repo(), schema, options)
      end

      @spec get_range(
              Ecto.Schema.t(),
              Tablestore.start_primary_keys(),
              Tablestore.end_primary_keys(),
              Tablestore.options()
            ) :: {nil, nil} | {list, nil} | {list, binary} | {:error, term}
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

      @spec stream(Ecto.Schema.t(), Tablestore.options()) :: Enumerable.t()
      def stream(schema, options \\ [direction: :forward]) do
        Tablestore.stream_range(get_dynamic_repo(), schema, options)
      end

      @spec stream_range(
              Ecto.Schema.t(),
              Tablestore.start_primary_keys(),
              Tablestore.end_primary_keys(),
              Tablestore.options()
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

      @spec batch_get(Tablestore.batch_gets()) :: {:ok, Keyword.t()} | {:error, term}
      def batch_get(gets), do: Tablestore.batch_get(get_dynamic_repo(), gets)

      ## Addition

      @spec batch_write(Tablestore.batch_writes(), Tablestore.options()) ::
              {:ok, Keyword.t()} | {:error, term}
      def batch_write(writes, options \\ []) do
        Tablestore.batch_write(get_dynamic_repo(), writes, options)
      end
    end
  end

  @impl true
  def checked_out?(_adapter_meta), do: false

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
          Ecto.Adapters.Tablestore.Supervisor.child_spec([]),
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

    options =
      case condition_when_put_row_with_auto_increment_pk(schema, options) do
        {:ok, options} -> options
        _ -> options
      end

    case ExAliyunOts.put_row(instance, schema_meta.source, pks, attrs, options) do
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

      {:error, %Error{code: @ots_condition_check_fail}} ->
        {:error, :stale}

      {:error, error} ->
        {:invalid, [{:check, error.code}]}
    end
  end

  @impl true
  def delete(repo, schema_meta, filters, options) do
    case ExAliyunOts.delete_row(
           repo.instance,
           schema_meta.source,
           primary_key_as_string(schema_meta.schema, filters),
           Keyword.take(options, [:condition, :transaction_id])
         ) do
      {:ok, _response} ->
        {:ok, []}

      {:error, error} ->
        case error do
          %Error{code: @ots_condition_check_fail} ->
            {:error, :stale}

          _ ->
            {:invalid, [{:check, error.code}]}
        end
    end
  end

  @impl true
  def update(repo, schema_meta, fields, ids, returning, options) do
    missing_fields_from_returning =
      Enum.find(fields, fn
        {field_name, {:increment, _}} ->
          field_name not in returning

        _ ->
          false
      end)

    if missing_fields_from_returning != nil do
      {field_name, _} = missing_fields_from_returning

      {:invalid,
       [
         {:check,
          "Require to set `#{inspect(field_name)}` in the :returning option of Repo update when using atomic increment operation"}
       ]}
    else
      schema = schema_meta.schema

      options =
        options
        |> Keyword.take([:condition, :transaction_id])
        |> Keyword.merge(map_attrs_to_update(schema, fields))

      options = may_put_optimistic_lock_into_condition(schema, ids, options)

      case ExAliyunOts.update_row(
             repo.instance,
             schema_meta.source,
             primary_key_as_string(schema, ids),
             options
           ) do
        {:ok, response} ->
          case response.row do
            nil ->
              {:ok, []}

            _ ->
              returning_from_response = extract_as_keyword(schema, response.row)

              related_fields_map =
                fields
                |> Keyword.merge(returning_from_response)
                |> Keyword.merge(ids)
                |> Map.new()

              # map the changed fields by the order of `returning`.
              return_fields =
                Enum.map(returning, fn field ->
                  {field, Map.get(related_fields_map, field)}
                end)

              {:ok, return_fields}
          end

        {:error, error} ->
          case error do
            %Error{code: @ots_condition_check_fail} ->
              {:error, :stale}

            _ ->
              {:invalid, [{:check, error.code}]}
          end
      end
    end
  end

  @impl true
  def insert_all(repo, schema_meta, header, list, on_conflict, returning, placeholders, options) do
    IO.puts(
      "insert_all - repo: #{inspect(repo)}, schema_meta: #{inspect(schema_meta)}, header: #{inspect(header)}, " <>
        "list: #{inspect(list)}, on_conflict: #{inspect(on_conflict)}, returning: #{inspect(returning)}, " <>
        "placeholders: #{inspect(placeholders)}, options: #{inspect(options)}\nplease use `batch_write` instead this function"
    )
  end

  @doc false
  def search(repo, schema, index_name, options) do
    meta = Ecto.Adapter.lookup_meta(repo)
    options = schema_fields_for_columns_to_get(schema, options)

    case ExAliyunOts.search(meta.instance, schema.__schema__(:source), index_name, options) do
      {:ok, response} ->
        {
          :ok,
          %{
            is_all_succeeded: response.is_all_succeeded,
            next_token: response.next_token,
            total_hits: response.total_hits,
            aggs: response.aggs,
            group_bys: response.group_bys,
            schemas: Enum.map(response.rows, &row_to_schema(schema, &1))
          }
        }

      error ->
        error
    end
  end

  @doc false
  def stream_search(repo, schema, index_name, options) do
    meta = Ecto.Adapter.lookup_meta(repo)
    options = schema_fields_for_columns_to_get(schema, options)

    ExAliyunOts.stream_search(
      meta.instance,
      schema.__schema__(:source),
      index_name,
      options
    )
    |> Stream.flat_map(fn
      {:ok, response} -> transfer_rows_by_schema(response.rows, schema, [])
      error -> [error]
    end)
  end

  @doc false
  def get(repo, schema, ids, options) do
    meta = Ecto.Adapter.lookup_meta(repo)
    options = schema_fields_for_columns_to_get(schema, options)

    case ExAliyunOts.get_row(
           meta.instance,
           schema.__schema__(:source),
           primary_key_as_string(schema, ids),
           options
         ) do
      {:ok, response} -> row_to_schema(schema, response.row)
      error -> error
    end
  end

  @doc false
  def get_range(repo, schema, options) do
    {schema, start_primary_keys, end_primary_keys} = get_range_params_by_schema(schema, options)
    get_range(repo, schema, start_primary_keys, end_primary_keys, options)
  end

  @doc false
  def get_range(repo, schema, start_primary_keys, end_primary_keys, options) do
    meta = Ecto.Adapter.lookup_meta(repo)
    options = schema_fields_for_columns_to_get(schema, options)

    result =
      ExAliyunOts.get_range(
        meta.instance,
        schema.__schema__(:source),
        prepare_start_primary_keys_by_order(schema, start_primary_keys),
        primary_key_as_string(schema, end_primary_keys),
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
  def stream_range(repo, schema, options) do
    {schema, start_primary_keys, end_primary_keys} = get_range_params_by_schema(schema, options)
    stream_range(repo, schema, start_primary_keys, end_primary_keys, options)
  end

  @doc false
  def stream_range(repo, schema, start_primary_keys, end_primary_keys, options) do
    meta = Ecto.Adapter.lookup_meta(repo)
    options = schema_fields_for_columns_to_get(schema, options)

    meta.instance
    |> ExAliyunOts.stream_range(
      schema.__schema__(:source),
      prepare_start_primary_keys_by_order(schema, start_primary_keys),
      primary_key_as_string(schema, end_primary_keys),
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
    Enum.map(rows, &row_to_schema(schema, &1))
  end

  defp get_range_params_by_schema(schema, options) do
    {schema, primary_keys} =
      if is_atom(schema) do
        {schema, schema |> struct() |> Ecto.primary_key()}
      else
        {schema.__struct__, Ecto.primary_key(schema)}
      end

    fun = fn fill ->
      fn
        {k, nil} -> {k, fill}
        kv -> kv
      end
    end

    {start_primary_keys, end_primary_keys} =
      case Keyword.get(options, :direction, :forward) do
        :forward ->
          {Enum.map(primary_keys, fun.(:inf_min)), Enum.map(primary_keys, fun.(:inf_max))}

        _ ->
          {Enum.map(primary_keys, fun.(:inf_max)), Enum.map(primary_keys, fun.(:inf_min))}
      end

    {schema, start_primary_keys, end_primary_keys}
  end

  @doc false
  def batch_get(repo, gets) do
    meta = Ecto.Adapter.lookup_meta(repo)
    {requests, schemas_mapping} = Enum.reduce(gets, {[], %{}}, &map_batch_gets/2)
    prepared_requests = Enum.reverse(requests)

    case ExAliyunOts.batch_get(meta.instance, prepared_requests) do
      {:ok, response} ->
        {:ok, batch_get_row_response_to_schemas(response.tables, schemas_mapping)}

      error ->
        error
    end
  end

  @doc false
  defdelegate batch_write(repo, writes, options), to: EctoTablestore.Repo.BatchWrite

  @doc false
  def generate_condition_options(%{__meta__: _meta} = entity, options) do
    condition =
      entity
      |> generate_filter_options(Keyword.take(options, [:entity_full_match]))
      |> merge_condition(Keyword.get(options, :condition))

    Keyword.put(options, :condition, condition)
  end

  @doc false
  def generate_condition_options(:put, %{__meta__: %{schema: schema}} = entity, options) do
    case condition_when_put_row_with_auto_increment_pk(schema, options) do
      {:ok, options} ->
        options

      _ ->
        # schema definition no server side auth increment primary_key
        generate_condition_options(entity, options)
    end
  end

  defp condition_when_put_row_with_auto_increment_pk(schema, options) do
    with {field, _, :id} <- schema.__schema__(:autogenerate_id),
         true <- define_auto_increment_pk?(schema, field) do
      # when put row with auto increment primary_key from server side,
      # must set `:IGNORE` condition.
      {
        :ok,
        Keyword.put(options, :condition, %Condition{row_existence: RowExistence.ignore()})
      }
    else
      _ ->
        options
    end
  end

  defp define_auto_increment_pk?(schema, field) do
    [_first_primary_key | other_pks] = schema.__schema__(:primary_key)
    field in other_pks
  end

  @doc false
  def generate_filter_options(%{__meta__: _meta} = entity, options) do
    case Keyword.pop(options, :entity_full_match, false) do
      {true, options} -> extract_filter_options_from_entity(entity, options)
      {_, options} -> options
    end
  end

  defp extract_filter_options_from_entity(entity, options) do
    attr_columns = entity_attr_columns(entity)

    options =
      attr_columns
      |> generate_filter_from_entity()
      |> filter_to_options(options)

    columns_to_get_opt = Keyword.get(options, :columns_to_get, [])

    if not is_list(columns_to_get_opt) do
      raise "Invalid usecase - require `columns_to_get` as list, but got: #{inspect(columns_to_get_opt)}"
    end

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

  defp generate_filter_from_entity(fields) do
    generate_filter_from_entity(fields, [])
  end

  defp generate_filter_from_entity([], []), do: nil

  defp generate_filter_from_entity([], [%Filter{type: FilterType.single_column()} = filter]) do
    filter
  end

  defp generate_filter_from_entity([], prepared) do
    %Filter{
      filter: %CompositeColumnValueFilter{
        combinator: :LO_AND,
        sub_filters: prepared
      },
      type: FilterType.composite_column()
    }
  end

  defp generate_filter_from_entity([{field_name, value} | rest], prepared)
       when is_map(value) or is_list(value) do
    filter = %Filter{
      filter: %SingleColumnValueFilter{
        column_name: Atom.to_string(field_name),
        column_value: Jason.encode!(value),
        comparator: ComparatorType.equal(),
        filter_if_missing: true,
        latest_version_only: true
      },
      type: FilterType.single_column()
    }

    generate_filter_from_entity(rest, [filter | prepared])
  end

  defp generate_filter_from_entity([{field_name, value} | rest], prepared) do
    filter = %Filter{
      filter: %SingleColumnValueFilter{
        column_name: Atom.to_string(field_name),
        column_value: value,
        comparator: ComparatorType.equal(),
        filter_if_missing: true,
        latest_version_only: true
      },
      type: FilterType.single_column()
    }

    generate_filter_from_entity(rest, [filter | prepared])
  end

  @doc false
  def key_to_global_sequence(table_name, field) do
    "#{table_name},#{field}"
  end

  defp merge_condition([], nil), do: nil
  defp merge_condition([], %Condition{} = condition), do: condition

  defp merge_condition([filter: filter_from_entity], nil) do
    %Condition{
      column_condition: filter_from_entity,
      row_existence: RowExistence.expect_exist()
    }
  end

  defp merge_condition([filter: filter_from_entity], %Condition{column_condition: nil}) do
    %Condition{
      column_condition: filter_from_entity,
      row_existence: RowExistence.expect_exist()
    }
  end

  defp merge_condition([filter: filter_from_entity], %Condition{
         column_condition: column_condition
       }) do
    %Condition{
      column_condition: do_generate_filter(filter_from_entity, :and, column_condition),
      row_existence: RowExistence.expect_exist()
    }
  end

  defp filter_to_options(filter_from_entity) do
    filter_to_options(filter_from_entity, [])
  end

  defp filter_to_options(nil, options), do: options

  defp filter_to_options(filter_from_entity, options) do
    merged = do_generate_filter(filter_from_entity, :and, options[:filter])
    Keyword.put(options, :filter, merged)
  end

  defp do_generate_filter(filter_from_entity, :and, nil), do: filter_from_entity

  defp do_generate_filter(filter_from_entity, :and, %Filter{} = filter_from_opt) do
    filter_names_from_opt = flatten_filter(filter_from_opt)

    filter_from_entity = do_drop_filter_from_entity(filter_from_entity, filter_names_from_opt)

    case filter_from_entity do
      filter_from_entity when is_list(filter_from_entity) ->
        %Filter{
          filter: %CompositeColumnValueFilter{
            combinator: LogicOperator.and(),
            sub_filters: Enum.reverse([filter_from_opt | filter_from_entity])
          },
          type: FilterType.composite_column()
        }

      %Filter{type: FilterType.single_column()} ->
        %Filter{
          filter: %CompositeColumnValueFilter{
            combinator: LogicOperator.and(),
            sub_filters: [filter_from_entity, filter_from_opt]
          },
          type: FilterType.composite_column()
        }

      nil ->
        filter_from_opt
    end
  end

  defp do_generate_filter(filter_from_entity, :or, %Filter{} = filter_from_opt) do
    %Filter{
      filter: %CompositeColumnValueFilter{
        combinator: LogicOperator.or(),
        sub_filters: [filter_from_entity, filter_from_opt]
      },
      type: FilterType.composite_column()
    }
  end

  defp do_generate_filter(_filter_from_entity, :and, filter_from_opt) do
    raise("Invalid usecase - input invalid `:filter` option: #{inspect(filter_from_opt)}")
  end

  defp flatten_filter(%Filter{
         filter: %{sub_filters: sub_filters},
         type: FilterType.composite_column()
       }) do
    sub_filters
    |> Enum.map(&flatten_filter/1)
    |> List.flatten()
  end

  defp flatten_filter(%Filter{
         filter: %{column_name: column_name},
         type: FilterType.single_column()
       }) do
    [column_name]
  end

  defp do_drop_filter_from_entity(
         %Filter{
           filter: %{column_name: column_name},
           type: FilterType.single_column()
         } = filter,
         fields
       ) do
    if column_name in fields, do: nil, else: filter
  end

  defp do_drop_filter_from_entity(
         %Filter{
           filter: %{sub_filters: sub_filters},
           type: FilterType.composite_column()
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

  def pks_and_attrs_to_put_row(instance, schema, fields) do
    prepare_primary_key_and_attribute_col_when_put_row(instance, schema, fields)
  end

  defp prepare_primary_key_and_attribute_col_when_put_row(instance, schema, fields) do
    # From the design principle of the primary key of table, there will only be
    # one `autogenerate_id` field at most.
    autogenerate_id = schema.__schema__(:autogenerate_id)

    {primary_keys, attribute_cols, autogen_field} =
      schema.__schema__(:primary_key)
      |> Enum.reduce({[], fields, nil}, fn primary_key, {primary_keys, fields, autogen_field} ->
        {value, fields} = Keyword.pop(fields, primary_key)

        {value, autogen_field} =
          value_of_primary_keys_to_put_row(
            instance,
            schema,
            primary_keys,
            primary_key,
            value,
            autogenerate_id,
            autogen_field
          )

        {[{Atom.to_string(primary_key), value} | primary_keys], fields, autogen_field}
      end)

    {
      Enum.reverse(primary_keys),
      map_attrs_to_row(schema, attribute_cols),
      autogen_field
    }
  end

  defp value_of_primary_keys_to_put_row(
         instance,
         schema,
         _,
         field,
         nil,
         {_, autogen_field, {:parameterized, Ecto.Hashids, _hashids} = type} = _autogenerate_id,
         _autogen_field
       )
       when field == autogen_field do
    source = schema.__schema__(:source)

    next_value =
      Sequence.next_value(instance, key_to_global_sequence(source, Atom.to_string(field)))

    {:ok, value} = Ecto.Type.dump(type, next_value)
    {value, field}
  end

  defp value_of_primary_keys_to_put_row(
         instance,
         schema,
         [],
         field,
         nil,
         {_, autogen_field, :id},
         _autogen_field
       )
       when field == autogen_field do
    # Set partition_key as an auto-generated, use sequence for this usecase
    source = schema.__schema__(:source)

    next_value =
      Sequence.next_value(instance, key_to_global_sequence(source, Atom.to_string(field)))

    {next_value, field}
  end

  defp value_of_primary_keys_to_put_row(
         _instance,
         _schema,
         prepared_primary_keys,
         field,
         nil,
         {_, autogen_field, :id},
         _autogen_field
       )
       when field == autogen_field and prepared_primary_keys != [] do
    # Exclude partition_key, start from the secondary primary key,
    # set the value as `PKType.auto_increment` to use the auto increment by server side
    {PKType.auto_increment(), field}
  end

  defp value_of_primary_keys_to_put_row(_instance, _schema, _, _field, value, _, autogen_field) do
    {value, autogen_field}
  end

  defp map_attrs_to_row(schema, attr_columns) do
    for {field, value} <- attr_columns do
      field_type = schema.__schema__(:type, field)
      do_map_attr_to_row_item(field_type, field, value)
    end
  end

  defp do_map_attr_to_row_item(:decimal, key, %Decimal{} = value) do
    {Atom.to_string(key), Decimal.to_string(value)}
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

  defp do_map_attr_to_row_item({:parameterized, Ecto.Embedded, %{cardinality: :one}}, key, value) do
    {Atom.to_string(key), Jason.encode!(value)}
  end

  defp do_map_attr_to_row_item({:parameterized, Ecto.Embedded, %{cardinality: :many}}, key, value) do
    {Atom.to_string(key), Jason.encode!(value)}
  end

  defp do_map_attr_to_row_item(_, key, value) do
    {Atom.to_string(key), value}
  end

  defp row_to_schema(_schema, nil), do: nil

  defp row_to_schema(schema, row) do
    struct(schema, extract_as_keyword(schema, row))
  end

  defp extract_as_keyword(schema, {nil, attrs}) do
    for {attr_key, attr_value, _ts} <- attrs do
      field = String.to_existing_atom(attr_key)
      type = schema.__schema__(:type, field)

      {
        field,
        load_field!(attr_value, type, field, schema)
      }
    end
  end

  defp extract_as_keyword(_schema, {pks, nil}) do
    for {pk_key, pk_value} <- pks do
      {String.to_existing_atom(pk_key), pk_value}
    end
  end

  defp extract_as_keyword(schema, {pks, attrs}) do
    pks =
      for {pk_key, pk_value} <- pks do
        {String.to_existing_atom(pk_key), pk_value}
      end

    attrs =
      for {attr_key, attr_value, _ts} <- attrs do
        field = String.to_existing_atom(attr_key)
        type = schema.__schema__(:type, field)

        {
          field,
          load_field!(attr_value, type, field, schema)
        }
      end

    Keyword.merge(attrs, pks)
  end

  defp load_field!(value, type, field, struct)
       when type == :naive_datetime_usec
       when type == :naive_datetime do
    value
    |> DateTime.from_unix!()
    |> DateTime.to_naive()
    |> load!(type, field, struct)
  end

  defp load_field!(value, type, field, struct)
       when type == :utc_datetime_usec
       when type == :utc_datetime do
    value
    |> DateTime.from_unix!()
    |> load!(type, field, struct)
  end

  defp load_field!(value, type, field, struct)
       when type == :map
       when type == :array do
    Jason.decode!(value) |> load!(type, field, struct)
  end

  defp load_field!(value, {:array, _} = type, field, struct) do
    Jason.decode!(value) |> load!(type, field, struct)
  end

  defp load_field!(value, {:map, _} = type, field, struct) do
    Jason.decode!(value) |> load!(type, field, struct)
  end

  defp load_field!(
         value,
         {:parameterized, Ecto.Embedded, %{cardinality: embedded_type, related: schema}},
         _field,
         _struct
       ) do
    decode_json_to_embedded(embedded_type, schema, value)
  end

  defp load_field!(value, :decimal, field, struct) when is_bitstring(value) do
    value |> Decimal.new() |> load_field!(:decimal, field, struct)
  end

  defp load_field!(value, type, field, struct) do
    load!(value, type, field, struct)
  end

  @compile {:inline, load!: 4}
  defp load!(value, type, field, struct) do
    case Ecto.Type.adapter_load(__MODULE__, type, value) do
      {:ok, value} ->
        value

      :error ->
        field = field && " for field #{inspect(field)}"
        struct = struct && " in #{inspect(struct)}"

        raise ArgumentError,
              "cannot load `#{inspect(value)}` as type #{inspect(type)}#{field}#{struct}"
    end
  end

  defp decode_json_to_embedded(:one, schema, value) do
    value
    |> Jason.decode!()
    |> embedded_load(schema)
  end

  defp decode_json_to_embedded(:many, schema, value) do
    value
    |> Jason.decode!()
    |> Enum.map(&embedded_load(&1, schema))
  end

  defp embedded_load(data, schema) do
    Ecto.embedded_load(schema, data, :json)
  end

  @doc false
  def map_attrs_to_update(schema, attrs) do
    {_, updates} = Enum.reduce(attrs, {schema, Keyword.new()}, &construct_row_updates/2)
    updates
  end

  defp construct_row_updates({field, nil}, {schema, acc}) when is_atom(field) do
    field_str = Atom.to_string(field)

    {
      schema,
      Keyword.update(acc, :delete_all, [field_str], &[field_str | &1])
    }
  end

  defp construct_row_updates({field, {:increment, value}}, {schema, acc})
       when is_integer(value) do
    field_str = Atom.to_string(field)

    acc =
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

    {schema, acc}
  end

  defp construct_row_updates({field, %Ecto.Changeset{valid?: true} = changeset}, {schema, acc}) do
    field_type = schema.__schema__(:type, field)
    embeds = schema.__schema__(:embeds)
    embeds = Ecto.Embedded.prepare(changeset, embeds, __MODULE__, :update)
    changes = Map.merge(changeset.changes, embeds)

    {
      schema,
      Keyword.update(
        acc,
        :put,
        [do_map_attr_to_row_item(field_type, field, changes)],
        &[do_map_attr_to_row_item(field_type, field, changes) | &1]
      )
    }
  end

  defp construct_row_updates({field, value}, {schema, acc}) when is_atom(field) do
    field_type = schema.__schema__(:type, field)

    {
      schema,
      Keyword.update(
        acc,
        :put,
        [do_map_attr_to_row_item(field_type, field, value)],
        &[do_map_attr_to_row_item(field_type, field, value) | &1]
      )
    }
  end

  defp prepare_start_primary_keys_by_order(schema, start_primary_keys)
       when is_list(start_primary_keys) do
    primary_key_as_string(schema, start_primary_keys)
  end

  defp prepare_start_primary_keys_by_order(_schema, start_primary_keys)
       when is_binary(start_primary_keys) do
    start_primary_keys
  end

  defp prepare_start_primary_keys_by_order(schema, start_primary_keys) do
    raise "Invalid start_primary_keys: #{inspect(start_primary_keys)} for #{schema}, expect it as `list` or `binary`"
  end

  defp primary_key_value(:inf_min), do: PKType.inf_min()
  defp primary_key_value(:inf_max), do: PKType.inf_max()
  defp primary_key_value(value), do: value

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
            primary_key_as_string(schema_entity),
            MapSet.put(acc, meta.schema)
          }
        end
      )

    if MapSet.size(source_set) > 1 do
      raise "Invalid usecase - input batch get request: #{inspect(schema_entities)} are different types of schema entity in batch."
    end

    {conflict_schemas, filters, columns_to_get} =
      Enum.reduce(schema_entities, {[], [], []}, fn schema_entity,
                                                    {conflict_schemas, filters, columns_to_get} ->
        options = generate_filter_options(schema_entity, options)

        prepared_columns_to_get =
          options
          |> Keyword.get(:columns_to_get, [])
          |> splice_list(columns_to_get)
          |> MapSet.new()
          |> MapSet.to_list()

        case Keyword.get(options, :filter) do
          nil ->
            {[schema_entity | conflict_schemas], filters, prepared_columns_to_get}

          filter ->
            {conflict_schemas, [filter | filters], prepared_columns_to_get}
        end
      end)

    if length(filters) != 0 and length(conflict_schemas) != 0 do
      raise "Invalid usecase - conflicts for schema_entities: #{inspect(conflict_schemas)}, " <>
              "they are only be with primary key(s), but generate filters: #{inspect(filters)} from " <>
              "other schema_entities attribute fields, please input schema_entities both have attribute fields " <>
              "or use the `filter` option."
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

    schema = MapSet.to_list(source_set) |> List.first()

    options =
      if columns_to_get != [] do
        Keyword.put(options, :columns_to_get, columns_to_get)
      else
        schema_fields_for_columns_to_get(schema, options)
      end

    source = schema.__schema__(:source)
    request = ExAliyunOts.get(source, ids_groups, options)
    {[request | requests], Map.put(schemas_mapping, source, schema)}
  end

  defp map_batch_gets({schema, ids_groups, options}, {requests, schemas_mapping})
       when is_list(ids_groups) do
    source = schema.__schema__(:source)
    options = schema_fields_for_columns_to_get(schema, options)
    request = ExAliyunOts.get(source, format_ids_groups(schema, ids_groups), options)
    {[request | requests], Map.put(schemas_mapping, source, schema)}
  end

  defp map_batch_gets({schema, ids_groups}, acc) when is_list(ids_groups) do
    map_batch_gets({schema, ids_groups, []}, acc)
  end

  defp map_batch_gets(request, _acc) do
    raise("Invalid usecase - input invalid batch get request: #{inspect(request)}")
  end

  defp format_ids_groups(schema, [ids | _] = ids_groups) when is_list(ids) do
    Enum.map(ids_groups, fn ids_group ->
      if is_list(ids_group),
        do: primary_key_as_string(schema, ids_group),
        else: primary_key_as_string(schema, [ids_group])
    end)
  end

  defp format_ids_groups(schema, ids_groups) when is_list(ids_groups) do
    primary_keys_size = length(schema.__schema__(:primary_key))

    if primary_keys_size == length(ids_groups) do
      # fetch a single row with multi primary_key(s)
      format_ids_groups(schema, [ids_groups])
    else
      # fetch multi rows with multi primary_keys
      Enum.map(ids_groups, &primary_key_as_string(schema, &1))
    end
  end

  @doc false
  def autogen_fields(schema) do
    case schema.__schema__(:autogenerate) do
      [{autogen_fields, {m, f, a}}] ->
        autogen_value = apply(m, f, a)
        Enum.map(autogen_fields, &{&1, autogen_value})

      _ ->
        []
    end
  end

  @doc false
  def autoupdate_fields(schema) do
    case schema.__schema__(:autoupdate) do
      [{autoupdate_fields, {m, f, a}}] ->
        autoupdate_value = apply(m, f, a)
        Enum.map(autoupdate_fields, &{&1, autoupdate_value})

      _ ->
        []
    end
  end

  defp entity_attr_columns(%{__meta__: meta} = entity) do
    meta.schema
    |> attribute_fields()
    |> Enum.reduce([], fn field_name, acc ->
      case Map.get(entity, field_name) do
        value when value != nil ->
          [{field_name, value} | acc]

        nil ->
          acc
      end
    end)
  end

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
        [] -> [{schema, nil} | acc]
        _ -> [{schema, Enum.reverse(schemas_data)} | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp splice_list(list1, list2) when is_list(list1) and is_list(list2) do
    List.flatten([list1 | list2])
  end

  defp may_put_optimistic_lock_into_condition(schema, ids, options) do
    case Keyword.drop(ids, schema.__schema__(:primary_key)) do
      [] ->
        options

      lock_fields ->
        condition =
          lock_fields
          |> generate_filter_from_entity()
          |> filter_to_options()
          |> merge_condition(options[:condition])

        Keyword.put(options, :condition, condition)
    end
  end

  @doc false
  def primary_key_as_string(struct) do
    struct
    |> Ecto.primary_key()
    |> Enum.map(fn {field, value} ->
      {Atom.to_string(field), primary_key_value(value)}
    end)
  end

  @doc false
  def primary_key_as_string(schema, pks) when is_list(pks) do
    map =
      Enum.reduce(pks, %{}, fn
        {pk_field, pk_value}, acc when is_atom(pk_field) ->
          Map.put(acc, Atom.to_string(pk_field), primary_key_value(pk_value))

        {pk_field, pk_value}, acc when is_bitstring(pk_field) ->
          Map.put(acc, pk_field, primary_key_value(pk_value))

        _, acc ->
          acc
      end)

    struct(schema)
    |> Ecto.primary_key()
    |> Enum.map(fn {field, _value} ->
      field = Atom.to_string(field)
      value = Map.get(map, field)
      {field, value}
    end)
  end

  @doc false
  def row_to_struct(%{__meta__: _} = struct, nil), do: struct
  def row_to_struct(struct, {nil, attrs}), do: reduce_items_into_struct(struct, attrs)
  def row_to_struct(struct, {pks, nil}), do: reduce_items_into_struct(struct, pks)

  def row_to_struct(struct, {pks, attrs}) do
    struct
    |> reduce_items_into_struct(attrs)
    |> reduce_items_into_struct(pks)
  end

  defp reduce_items_into_struct(%{__meta__: meta} = struct, items) do
    Enum.reduce(items, struct, fn
      {field, value, _ts}, acc ->
        field = String.to_existing_atom(field)
        schema = meta.schema
        type = schema.__schema__(:type, field)
        value = load_field!(value, type, field, schema)
        Map.put(acc, field, value)

      {field, value}, acc ->
        field = String.to_existing_atom(field)
        Map.put(acc, field, value)
    end)
  end

  defp schema_fields_for_columns_to_get(schema, []) do
    [columns_to_get: attribute_fields_to_string_list(schema)]
  end

  defp schema_fields_for_columns_to_get(schema, options) do
    options
    |> Keyword.get(:columns_to_get)
    |> put_columns_to_get(schema, options)
  end

  defp put_columns_to_get(nil, schema, options) do
    Keyword.put(options, :columns_to_get, attribute_fields_to_string_list(schema))
  end

  defp put_columns_to_get(value, schema, options)
       when value == :all
       when value == :RETURN_ALL do
    # used for search index function, e.g. [columns_to_get: :all]
    Keyword.put(options, :columns_to_get, attribute_fields_to_string_list(schema))
  end

  defp put_columns_to_get(value, schema, options)
       when value == :all_from_index
       when value == :RETURN_ALL_FROM_INDEX do
    # used for parallel scan function (still base on search index),
    # e.g. [columns_to_get: :all_from_index]
    Keyword.put(options, :columns_to_get, attribute_fields_to_string_list(schema))
  end

  defp put_columns_to_get(_value, _schema, options) do
    options
  end

  defp attribute_fields_to_string_list(schema) do
    schema
    |> attribute_fields()
    |> Enum.map(&Atom.to_string/1)
  end

  defp attribute_fields(schema) do
    schema.__schema__(:fields) -- schema.__schema__(:primary_key)
  end

  ## Storage

  @impl true
  def storage_up(opts), do: impl_storage_tips(opts, "create")
  @impl true
  def storage_down(opts), do: impl_storage_tips(opts, "drop")
  @impl true
  def storage_status(opts), do: impl_storage_tips(opts, "check")

  defp impl_storage_tips(opts, action) do
    msg =
      opts
      |> Keyword.get(:instance)
      |> error_msg_to_storage_tips(action)

    {:error, msg}
  end

  defp error_msg_to_storage_tips(nil, _action),
    do: """
      \n\nPlease refer https://hexdocs.pm/ecto_tablestore/readme.html#usage to configure your instance.
    """

  defp error_msg_to_storage_tips(_instance, action),
    do: """
      \n\nPlease #{action} tablestore instance/database visit Alibaba TableStore product console through
      https://otsnext.console.aliyun.com/
    """
end

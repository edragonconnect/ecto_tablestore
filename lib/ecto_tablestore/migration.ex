defmodule EctoTablestore.Migration do
  @moduledoc """
  Migrations are used to create your tables.

  Support the partition key is autoincrementing based on this library's wrapper, for this use
  case, we can use the migration to automatically create an another separated table to generate
  the serial value when `:insert` (viz `ExAliyunOts.put_row/5`) or `:batch_write` (viz
  `ExAliyunOts.batch_write/3`) with `:put` option.

  In practice, we don't create migration files by hand either, we typically use `mix
  ecto.ots.gen.migration` to generate the file with the proper timestamp and then we just fill in
  its contents:

      $ mix ecto.ots.gen.migration create_posts_table

  And then we can fill the table definition details:

      defmodule EctoTablestore.TestRepo.Migrations.CreatePostsTable do
        use EctoTablestore.Migration

        def change do
          create table("ecto_ots_test_posts") do
            add :post_id, :integer, partition_key: true, auto_increment: true
          end
        end
      end


  After we filled the above migration content, you can run the migration above by going to the
  root of your project and typing:

      $ mix ecto.ots.migrate

  Finally, we successfully create the "ecto_ots_test_posts" table, since the above definition
  added an autoincrementing column for the partition key, there will automatically create an
  "ecto_ots_test_posts_seq" table to generate a serial integer for `:post_id` field when insert a
  new record.
  """
  require ExAliyunOts.Const.PKType, as: PKType
  require Logger
  alias EctoTablestore.{Sequence, Migration.Runner}
  alias Ecto.MigrationError
  alias ExAliyunOts.Var.Search

  defmodule Table do
    @moduledoc false

    defstruct name: nil, prefix: nil, partition_key: true, meta: []

    @type t :: %__MODULE__{
            name: String.t(),
            prefix: String.t() | nil,
            partition_key: boolean(),
            meta: Keyword.t()
          }
  end

  defmodule SecondaryIndex do
    @moduledoc false

    defstruct table_name: nil, index_name: nil, prefix: nil, include_base_data: true

    @type t :: %__MODULE__{
            table_name: String.t(),
            index_name: String.t(),
            prefix: String.t() | nil,
            include_base_data: boolean()
          }
  end

  defmodule SearchIndex do
    @moduledoc false

    defstruct table_name: nil, index_name: nil, prefix: nil

    @type t :: %__MODULE__{
            table_name: String.t(),
            index_name: String.t(),
            prefix: String.t() | nil
          }
  end

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      import EctoTablestore.Migration,
        only: [
          table: 1,
          table: 2,
          secondary_index: 2,
          secondary_index: 3,
          search_index: 2,
          search_index: 3,
          create: 2,
          drop: 1,
          add: 2,
          add: 3,
          add_pk: 1,
          add_pk: 2,
          add_pk: 3,
          add_column: 1,
          add_column: 2,
          add_index: 3
        ]

      import ExAliyunOts.Search

      def __migration__, do: :ok
    end
  end

  @doc """
  Returns a table struct that can be given to `create/2`.

  Since Tablestore is a NoSQL service, there are up to 4 primary key(s) can be added when
  creation, the first added key is partition key when set `partition_key` option as false.

  ## Examples

      create table("products") do
        add :name, :string
        add :price, :integer
      end

      create table("products", partition_key: false) do
        add :name, :string
        add :price, :integer
      end

  ## Options

    * `:partition_key` - as `true` by default, and there will add an `:id` field as partition key
      with type as a large autoincrementing integer (as `bigserial`), Tablestore does not support
      `bigserial` type for primary keys, but can use the `ex_aliyun_ots` lib's wrapper - Sequence to
      implement it; when `false`, a partition key field is not generated on table creation.
    * `:prefix` - the prefix for the table.
    * `:meta` - define the meta information when create table, can see Tablestore's document for details:

    * `:reserved_throughput_write` - reserve the throughput for write when create table, an
      integer, the default value is 0;
    * `:reserved_throughput_read` - reserve the throughput for read when create table, an integer,
      the default value is 0;
    * `:time_to_live` - the survival time of the saved data, a.k.a TTL; an integer, unit as second,
      the default value is -1 (permanent preservation);
    * `:deviation_cell_version_in_sec` - maximum version deviation, the default value is 86400
      seconds, which is 1 day;
    * `stream_spec` - set the stream specification of Tablestore:

      - `is_enabled`, open or close stream
      - `expiration_time`, the expiration time of the table's stream

  """
  def table(name, opts \\ [])

  def table(name, opts) when is_atom(name) do
    table(Atom.to_string(name), opts)
  end

  def table(name, opts) when is_binary(name) and is_list(opts) do
    struct(%Table{name: name}, opts)
  end

  @doc """
  Returns a secondary index struct that can be given to `create/2`.

  For more information see the  [Chinese Docs](https://help.aliyun.com/document_detail/91947.html) | [English Docs](https://www.alibabacloud.com/help/doc-detail/91947.html)

  ## Examples

      create secondary_index("posts", "posts_owner") do
        add_pk(:owner_id)
        add_pk(:id)
        add_column(:title)
        add_column(:content)
      end

  ## Options

    * `:include_base_data`, specifies whether the index table includes the existing data in the base table, if set it to
    `true` means the index includes the existing data, if set it to `false` means the index excludes the existing data,
    optional, by default it is `true`.

  """
  def secondary_index(table_name, index_name, opts \\ [])
      when is_binary(table_name) and is_binary(index_name) and is_list(opts) do
    struct(%SecondaryIndex{table_name: table_name, index_name: index_name}, opts)
  end

  @doc """
  Returns a search index struct that can be given to `create/2`.

  For more information see the  [Chinese Docs](https://help.aliyun.com/document_detail/117452.html) | [English Docs](https://www.alibabacloud.com/help/doc-detail/117452.html)

  ## Examples

      create search_index("posts", "posts_owner") do
        field_schema_keyword("title")
        field_schema_keyword("content")
        field_sort("title")
      end

  """
  def search_index(table_name, index_name, opts \\ [])
      when is_binary(table_name) and is_binary(index_name) and is_list(opts) do
    struct(%SearchIndex{table_name: table_name, index_name: index_name}, opts)
  end

  @doc """
  Adds a primary key when creating a secondary index.
  """
  defmacro add_pk(column) when is_binary(column), do: quote(do: {:pk, column})

  defmacro add_pk(column) when is_atom(column),
    do: quote(do: {:pk, unquote(Atom.to_string(column))})

  defmacro add_pk(column) do
    raise ArgumentError,
          "error type when defining pk column: #{inspect(column)} for secondary_index, only supported one of type: [:binary, :atom]"
  end

  @doc """
  Adds a pre-defined column when creating a secondary index.
  """
  defmacro add_column(column) when is_binary(column), do: quote(do: {:column, column})

  defmacro add_column(column) when is_atom(column),
    do: quote(do: {:column, unquote(Atom.to_string(column))})

  defmacro add_column(column) do
    raise ArgumentError,
          "error type when defining pre-defined column: #{inspect(column)} for secondary_index, only supported one of type: [:binary, :atom]"
  end

  @doc """
  Define the primary key(s) of the table to create.

  By default, the table will also include an `:id` primary key field (it is also partition key)
  that has a type of `:integer` which is an autoincrementing column. Check the `table/2` docs for
  more information.

  There are up to 4 primary key(s) can be added when creation.

  ## Example

      create table("posts") do
        add :title, :string
      end

      # The above is equivalent to

      create table("posts") do
        add :id, :integer, partition_key: true, auto_increment: true
        add :title, :string
      end

  """
  defmacro create(object, do: block), do: expand_create(object, block)

  defp expand_create(object, block) do
    columns =
      case block do
        {:__block__, _, columns} -> columns
        column -> [column]
      end

    quote do
      map = unquote(__MODULE__).__create__(unquote(object), unquote(columns))
      Runner.push_command(&unquote(__MODULE__).do_create(&1, map))
    end
  end

  def __create__(%Table{} = table, columns) do
    {index_metas, columns} = Enum.split_with(columns, &is_tuple(&1))

    %{primary_key: pk_columns, pre_defined_column: pre_defined_columns} =
      Map.merge(
        %{primary_key: [], pre_defined_column: []},
        Enum.group_by(columns, & &1.column_type)
      )

    partition_key_count = Enum.count(pk_columns, & &1.partition_key)

    pk_columns =
      cond do
        partition_key_count == 1 ->
          pk_columns

        # Make the partition key as `:id` and in an increment integer sequence
        partition_key_count == 0 and table.partition_key ->
          opts = Runner.repo_config(:migration_primary_key, [])
          {name, opts} = Keyword.pop(opts, :name, "id")
          {type, _opts} = Keyword.pop(opts, :type, :integer)

          [
            %{
              name: name,
              type: type,
              column_type: :primary_key,
              partition_key: true,
              auto_increment: true
            }
            | pk_columns
          ]

        # No partition key defined
        partition_key_count == 0 ->
          raise MigrationError,
            message: "Please define at least one partition primary keys for table: " <> table.name

        # The partition key only can define one
        true ->
          raise MigrationError,
            message:
              "The maximum number of partition primary keys is 1, now is #{partition_key_count} defined on table: " <>
                table.name <> " columns:\n" <> inspect(pk_columns)
      end

    case Enum.count(pk_columns) do
      # The number of primary keys can not be more than 4
      pk_count when pk_count > 4 ->
        raise MigrationError,
          message:
            "The maximum number of primary keys is 4, now is #{pk_count} defined on table: " <>
              table.name <> " columns:\n" <> inspect(pk_columns)

      # Only support to define one primary key as auto_increment integer
      _pk_count ->
        %{hashids: hashids_count, auto_increment: auto_increment_count} =
          Enum.reduce(pk_columns, %{hashids: 0, auto_increment: 0, none: 0}, fn
            %{type: :hashids}, acc -> Map.update!(acc, :hashids, &(&1 + 1))
            %{auto_increment: true}, acc -> Map.update!(acc, :auto_increment, &(&1 + 1))
            _, acc -> Map.update!(acc, :none, &(&1 + 1))
          end)

        if (total_increment_count = auto_increment_count + hashids_count) > 1 do
          raise MigrationError,
            message:
              "The maximum number of [auto_increment & hashids] primary keys is 1, but now find #{
                total_increment_count
              } primary keys defined on table: " <> table.name
        else
          seq_type =
            cond do
              auto_increment_count > 0 -> :self_seq
              hashids_count > 0 -> :default_seq
              true -> :none_seq
            end

          %{
            table: table,
            pk_columns: pk_columns,
            pre_defined_columns: pre_defined_columns,
            index_metas: index_metas,
            seq_type: seq_type
          }
        end
    end
  end

  def __create__(%SecondaryIndex{} = secondary_index, columns) do
    g_columns = Enum.group_by(columns, &elem(&1, 0), &elem(&1, 1))

    case Map.keys(g_columns) -- [:pk, :column] do
      [] ->
        :ok

      [missing] ->
        raise MigrationError,
              "Missing #{missing} definition when creating: #{inspect(secondary_index)}, please use add_#{
                missing
              }/1 when creating secondary index."

      _ ->
        raise MigrationError,
              "Missing pk & column definition when creating: #{inspect(secondary_index)}, please use add_pk/1 and add_column/1 when creating secondary index."
    end

    %{
      secondary_index: secondary_index,
      primary_keys: g_columns.pk,
      defined_columns: g_columns.column
    }
  end

  def __create__(%SearchIndex{} = search_index, columns) do
    group_key = fn column ->
      if column.__struct__ in [
           Search.PrimaryKeySort,
           Search.FieldSort,
           Search.GeoDistanceSort,
           Search.ScoreSort
         ] do
        :index_sorts
      else
        :field_schemas
      end
    end

    g_columns = Enum.group_by(columns, group_key)

    unless Map.get(g_columns, :field_schemas) do
      raise MigrationError,
            "Missing field_schemas definition when creating: #{inspect(search_index)}, please use field_schema_* functions when creating search index."
    end

    %{
      search_index: search_index,
      field_schemas: g_columns.field_schemas,
      index_sorts: g_columns[:index_sorts] || []
    }
  end

  @doc false
  # create table
  def do_create(repo, %{
        table: table,
        pk_columns: pk_columns,
        pre_defined_columns: pre_defined_columns,
        index_metas: index_metas,
        seq_type: seq_type
      }) do
    table_name = get_table_name(table, repo.config())
    table_name_str = IO.ANSI.format([:green, table_name, :reset])
    repo_meta = Ecto.Adapter.lookup_meta(repo)
    instance = repo_meta.instance
    primary_keys = Enum.map(pk_columns, &transform_table_column/1)
    defined_columns = Enum.map(pre_defined_columns, &transform_table_column/1)

    print_list =
      Enum.reject(
        [
          primary_keys: primary_keys,
          defined_columns: defined_columns,
          index_metas: index_metas
        ],
        &match?({_, []}, &1)
      )

    Logger.info(fn ->
      ">> creating table: #{table_name_str} by #{
        inspect(print_list, pretty: true, limit: :infinity)
      } "
    end)

    options =
      Keyword.merge(table.meta,
        max_versions: 1,
        defined_columns: defined_columns,
        index_metas: index_metas
      )

    case ExAliyunOts.create_table(instance, table_name, primary_keys, options) do
      :ok ->
        result_str = IO.ANSI.format([:green, "ok", :reset])
        Logger.info(fn -> ">>>> create table: #{table_name_str} result: #{result_str}" end)

        create_seq_table_by_type!(seq_type, table_name, repo, instance)
        :ok

      {:error, error} ->
        raise MigrationError, "create table: #{table_name} error: " <> error.message
    end
  end

  # create secondary_index
  def do_create(repo, %{
        secondary_index: secondary_index,
        primary_keys: primary_keys,
        defined_columns: defined_columns
      }) do
    {table_name, index_name} = get_index_name(secondary_index, repo.config())
    table_name_str = IO.ANSI.format([:green, table_name, :reset])
    index_name_str = IO.ANSI.format([:green, index_name, :reset])
    include_base_data = secondary_index.include_base_data
    repo_meta = Ecto.Adapter.lookup_meta(repo)

    Logger.info(fn ->
      ">> creating secondary_index: #{index_name_str} for table: #{table_name_str} by #{
        inspect(
          [
            primary_keys: primary_keys,
            defined_columns: defined_columns,
            include_base_data: include_base_data
          ],
          pretty: true,
          limit: :infinity
        )
      } "
    end)

    case ExAliyunOts.create_index(
           repo_meta.instance,
           table_name,
           index_name,
           primary_keys,
           defined_columns,
           include_base_data: include_base_data
         ) do
      :ok ->
        result_str = IO.ANSI.format([:green, "ok", :reset])

        Logger.info(fn ->
          ">>>> create secondary_index: #{index_name_str} for table: #{table_name_str} result: #{
            result_str
          }"
        end)

        :ok

      {:error, error} ->
        raise MigrationError,
              "create secondary index: #{index_name} for table: #{table_name} error: " <>
                error.message
    end
  end

  # create search_index
  def do_create(repo, %{
        search_index: search_index,
        field_schemas: field_schemas,
        index_sorts: index_sorts
      }) do
    {table_name, index_name} = get_index_name(search_index, repo.config())
    table_name_str = IO.ANSI.format([:green, table_name, :reset])
    index_name_str = IO.ANSI.format([:green, index_name, :reset])
    repo_meta = Ecto.Adapter.lookup_meta(repo)

    Logger.info(fn ->
      ">> creating search index: #{index_name_str} for table: #{table_name_str} by #{
        inspect(
          [field_schemas: field_schemas, index_sorts: index_sorts],
          pretty: true,
          limit: :infinity
        )
      } "
    end)

    case ExAliyunOts.create_search_index(
           repo_meta.instance,
           table_name,
           index_name,
           field_schemas: field_schemas,
           index_sorts: index_sorts
         ) do
      {:ok, _} ->
        result_str = IO.ANSI.format([:green, "ok", :reset])

        Logger.info(fn ->
          ">>>> create search index: #{index_name_str} for table: #{table_name_str} result: #{
            result_str
          }"
        end)

        :ok

      {:error, error} ->
        raise MigrationError,
              "create search index: #{index_name} for table: #{table_name} error: " <>
                error.message
    end
  end

  defp create_seq_table_by_type!(:none_seq, _table_name, _repo, _instance),
    do: :ignore

  defp create_seq_table_by_type!(seq_type, table_name, repo, instance) do
    seq_table_name =
      case seq_type do
        :self_seq -> repo.__adapter__.bound_sequence_table_name(table_name)
        :default_seq -> Sequence.default_table()
      end

    # check if not exists
    with {:list_table, {:ok, %{table_names: table_names}}} <-
           {:list_table, ExAliyunOts.list_table(instance)},
         true <- seq_table_name not in table_names,
         :ok <-
           ExAliyunOts.Sequence.create(instance, %ExAliyunOts.Var.NewSequence{
             name: seq_table_name
           }) do
      Logger.info(fn ->
        ">> auto create table: #{seq_table_name} for table: " <> table_name
      end)

      :ok
    else
      {:list_table, {:error, error}} ->
        raise MigrationError, "list_table error: " <> error.message

      {:error, error} ->
        raise MigrationError, "create table: #{seq_table_name} error: " <> error.message

      false ->
        :already_exists
    end
  end

  @doc false
  defp get_table_name(%{prefix: prefix, name: name}, repo_config) do
    prefix = prefix || Keyword.get(repo_config, :migration_default_prefix)

    if prefix do
      prefix <> name
    else
      name
    end
  end

  defp get_index_name(
         %{prefix: prefix, table_name: table_name, index_name: index_name},
         repo_config
       ) do
    prefix = prefix || Keyword.get(repo_config, :migration_default_prefix)

    if prefix do
      {prefix <> table_name, prefix <> index_name}
    else
      {table_name, index_name}
    end
  end

  defp transform_table_column(%{column_type: :pre_defined_column, name: field_name, type: type}) do
    {field_name, type}
  end

  defp transform_table_column(%{
         column_type: :primary_key,
         name: field_name,
         type: type,
         partition_key: partition_key?,
         auto_increment: auto_increment?
       }) do
    case type do
      :integer when auto_increment? and not partition_key? ->
        {field_name, PKType.integer(), PKType.auto_increment()}

      _ ->
        type_mapping = %{
          hashids: PKType.string(),
          integer: PKType.integer(),
          string: PKType.string(),
          binary: PKType.binary()
        }

        {field_name, type_mapping[type]}
    end
  end

  @doc """
  Adds a primary key when creating a table.

  This function only accepts types as `:string` | `:binary` | `:integer` | `:hashids`.

  About `:auto_increment` option:

    * set `:auto_increment` as `true` and its field is primary key of non-partitioned key, there
      will use Tablestore's auto-increment column to process it.

    * set `:auto_increment` as `true` and its field is partition key, there will use
      `ex_aliyun_ots`'s built-in Sequence function, the actual principle behind it is to use the
      atomic update operation though another separate table when generate serial integer, by default
      there will add an `:id` partition key as `:integer` type, the initial value of the sequence is
      0, and the increment step is 1.

  Tablestore can only have up to 4 primary keys, meanwhile the first defined primary key is the
  partition key, Please know that the order of the primary key definition will be directly mapped
  to the created table.

  About `:hashids` type to define the partition key:

    * set `partition_key` as `true` is required.
    * set `auto_increment` as `true` is required.

  ## Examples

  The auto generated serial integer for partition key:

      create table("posts") do
        add :title, :string
      end

      # The above is equivalent to

      create table("posts", partition_key: false) do
        add :id, :integer, partition_key: true, auto_increment: true
        add :title, :string
      end

  The explicitly defined field with `partition_key`:

      create table("posts") do
        add :title, :string
      end

      # The above is equivalent to

      create table("posts") do
        add :id, :integer, partition_key: true, auto_increment: true
        add :title, :string
      end

  The `:auto_increment` integer for primary key of non-partitioned key:

      create table("posts") do
        add :tag, :integer, auto_increment: true
      end

      # The above is equivalent to

      create table("posts", partition_key: false) do
        add :id, :integer, partition_key: true, auto_increment: true
        add :version, :integer, auto_increment: true
      end

  The `:hashids` type for the partition key with the built-in sequence feature:

      create table("posts") do
        add :id, :hashids, auto_increment: true, partition_key: true
      end

  ## Options

    * `:partition_key` - when `true`, marks this field as the partition key, only the first
      explicitly defined field is available for this option.
    * `:auto_increment` - when `true` and this field is non-partitioned key, Tablestore
      automatically generates the primary key value, which is unique in the partition key, and which
      increases progressively, when `true` and this field is a partition key, use `ex_aliyun_ots`'s
      Sequence to build a serial number for this field, the `auto_increment: true` option only
      allows binding of one primary key.

  """
  defmacro add(column, type, opts \\ []), do: _add_pk(column, type, opts)

  @doc """
  Adds a primary key when creating a table.

  Same as `add/2`, see `add/2` for more information.
  """
  defmacro add_pk(column, type, opts \\ []), do: _add_pk(column, type, opts)

  defp _add_pk(column, type, opts)
       when (is_atom(column) or is_binary(column)) and is_list(opts) do
    validate_pk_type!(column, type)

    quote location: :keep do
      %{
        name: unquote(to_string(column)),
        type: unquote(type),
        column_type: :primary_key,
        partition_key: Keyword.get(unquote(opts), :partition_key, false),
        auto_increment: Keyword.get(unquote(opts), :auto_increment, false)
      }
    end
  end

  defp validate_pk_type!(column, type) do
    # more information can be found in the [documentation](https://help.aliyun.com/document_detail/106536.html)
    if type in [:integer, :string, :binary, :hashids] do
      :ok
    else
      raise ArgumentError,
            "#{inspect(type)} is not a valid primary key type for column: `#{inspect(column)}`, " <>
              "please use an atom as :integer | :string | :binary | :hashids ."
    end
  end

  @doc """
  Adds a pre-defined column when creating a table.

  This function only accepts types as `:integer` | `:double` | `:boolean` | `:string` | `:binary`.

  For more information see the  [Chinese Docs](https://help.aliyun.com/document_detail/91947.html) | [English Docs](https://www.alibabacloud.com/help/doc-detail/91947.html)

  ## Examples

      create table("posts") do
        add_pk(:id, :integer, partition_key: true)
        add_pk(:owner_id, :string)
        add_column(:title, :string)
        add_column(:content, :string)
      end

  """
  defmacro add_column(column, type), do: _add_column(column, type)

  defp _add_column(column, type) when is_atom(column) or is_binary(column) do
    validate_pre_defined_col_type!(column, type)

    quote location: :keep do
      %{
        name: unquote(to_string(column)),
        type: unquote(type),
        column_type: :pre_defined_column
      }
    end
  end

  defp validate_pre_defined_col_type!(column, type) do
    # more information can be found in the [documentation](https://help.aliyun.com/document_detail/106536.html)
    if type in [:integer, :double, :boolean, :string, :binary] do
      :ok
    else
      raise ArgumentError,
            "#{inspect(type)} is not a valid pre-defined column type for column: `#{
              inspect(column)
            }`, " <>
              "please use an atom as :integer | :double | :boolean | :string | :binary ."
    end
  end

  @doc """
  Adds a secondary index when creating a table.

  For more information see the [Chinese Docs](https://help.aliyun.com/document_detail/91947.html) | [English Docs](https://www.alibabacloud.com/help/doc-detail/91947.html)

  ## Examples

      create table("posts") do
        add_pk(:id, :integer, partition_key: true)
        add_pk(:owner_id, :string)
        add_column(:title, :string)
        add_column(:content, :string)
        add_index("posts_owner", [:owner_id, :id], [:title, :content])
        add_index("posts_title", [:title, :id], [:content])
      end

  """
  defmacro add_index(index_name, primary_keys, defined_columns)
           when is_binary(index_name) and is_list(primary_keys) and is_list(defined_columns) do
    check_and_transform_columns = fn columns ->
      columns
      |> Macro.prewalk(&Macro.expand(&1, __CALLER__))
      |> Enum.map(fn
        column when is_binary(column) ->
          column

        column when is_atom(column) ->
          Atom.to_string(column)

        column ->
          raise ArgumentError,
                "error type when defining column: #{inspect(column)} for add_index: #{index_name}, " <>
                  "only supported one of type: [:binary, :atom]"
      end)
    end

    quote location: :keep do
      {
        unquote(index_name),
        unquote(check_and_transform_columns.(primary_keys)),
        unquote(check_and_transform_columns.(defined_columns))
      }
    end
  end

  @doc """
  Drops one of the following:

    * a table
    * a secondary index

  ## Examples

      drop table("posts")
      drop secondary_index("posts", "posts_owner")
      drop search_index("posts", "posts_index")

  """
  def drop(%Table{} = table) do
    Runner.push_command(fn repo ->
      table_name = get_table_name(table, repo.config())
      table_name_str = IO.ANSI.format([:green, table_name, :reset])
      repo_meta = Ecto.Adapter.lookup_meta(repo)
      instance = repo_meta.instance

      Logger.info(fn -> ">> dropping table: #{table_name_str}" end)

      case ExAliyunOts.delete_table(instance, table_name) do
        :ok ->
          result_str = IO.ANSI.format([:green, "ok", :reset])
          Logger.info(fn -> ">>>> dropping table: #{table_name_str} result: #{result_str}" end)
          :ok

        {:error, error} ->
          raise MigrationError, "dropping table: #{table_name} error: " <> error.message
      end
    end)
  end

  def drop(%SecondaryIndex{} = secondary_index) do
    Runner.push_command(fn repo ->
      {table_name, index_name} = get_index_name(secondary_index, repo.config())
      table_name_str = IO.ANSI.format([:green, table_name, :reset])
      index_name_str = IO.ANSI.format([:green, index_name, :reset])
      repo_meta = Ecto.Adapter.lookup_meta(repo)

      Logger.info(fn ->
        ">> dropping secondary_index table: #{table_name_str}, index: #{index_name_str}"
      end)

      case ExAliyunOts.delete_index(repo_meta.instance, table_name, index_name) do
        :ok ->
          result_str = IO.ANSI.format([:green, "ok", :reset])

          Logger.info(fn ->
            ">>>> dropping secondary_index table: #{table_name_str}, index: #{index_name_str} result: #{
              result_str
            }"
          end)

          :ok

        {:error, error} ->
          raise MigrationError,
                "dropping secondary_index index: #{index_name} for table: #{table_name} error: " <>
                  error.message
      end
    end)
  end

  def drop(%SearchIndex{} = search_index) do
    Runner.push_command(fn repo ->
      {table_name, index_name} = get_index_name(search_index, repo.config())
      table_name_str = IO.ANSI.format([:green, table_name, :reset])
      index_name_str = IO.ANSI.format([:green, index_name, :reset])
      repo_meta = Ecto.Adapter.lookup_meta(repo)

      Logger.info(fn ->
        ">> dropping search index table: #{table_name_str}, index: #{index_name_str}"
      end)

      case ExAliyunOts.delete_search_index(repo_meta.instance, table_name, index_name) do
        {:ok, _} ->
          result_str = IO.ANSI.format([:green, "ok", :reset])

          Logger.info(fn ->
            ">>>> dropping search index table: #{table_name_str}, index: #{index_name_str} result: #{
              result_str
            }"
          end)

          :ok

        {:error, error} ->
          raise MigrationError,
                "dropping search index index: #{index_name} for table: #{table_name} error: " <>
                  error.message
      end
    end)
  end
end

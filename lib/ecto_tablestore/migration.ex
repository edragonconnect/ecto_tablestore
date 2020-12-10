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
  alias EctoTablestore.Migration.Runner
  alias EctoTablestore.Sequence
  alias Ecto.MigrationError

  defmodule Table do
    @moduledoc false

    defstruct name: nil, prefix: nil, partition_key: true, meta: []

    @type t :: %__MODULE__{
            name: String.t(),
            prefix: atom | nil,
            partition_key: boolean(),
            meta: Keyword.t()
          }
  end

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      import EctoTablestore.Migration,
        only: [
          table: 1,
          table: 2,
          create: 2,
          add: 2,
          add: 3,
          add_pk: 2,
          add_pk: 3
        ]

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
  defmacro create(table, do: block), do: _create_table(table, block)

  defp _create_table(table, block) do
    columns =
      case block do
        {:__block__, _, columns} -> columns
        column -> [column]
      end

    quote do
      map = unquote(__MODULE__).__create_table__(unquote(table), unquote(columns))
      Runner.push_command(&unquote(__MODULE__).do_create_table(&1, map))
    end
  end

  def __create_table__(%Table{} = table, columns) do
    partition_key_count = Enum.count(columns, & &1.partition_key)

    columns =
      cond do
        partition_key_count == 1 ->
          columns

        # Make the partition key as `:id` and in an increment integer sequence
        partition_key_count == 0 and table.partition_key ->
          opts = Runner.repo_config(:migration_primary_key, [])
          {name, opts} = Keyword.pop(opts, :name, :id)
          {type, _opts} = Keyword.pop(opts, :type, :integer)
          [%{pk_name: name, type: type, partition_key: true, auto_increment: true} | columns]

        # No partition key defined
        partition_key_count == 0 ->
          raise MigrationError,
            message: "Please define at least one partition primary keys for table: " <> table.name

        # The partition key only can define one
        true ->
          raise MigrationError,
            message:
              "The maximum number of partition primary keys is 4, now is #{partition_key_count} defined on table: " <>
                table.name <> " columns:\n" <> inspect(columns)
      end

    case Enum.count(columns) do
      # The number of primary keys can not be more than 4
      pk_count when pk_count > 4 ->
        raise MigrationError,
          message:
            "The maximum number of primary keys is 4, now is #{pk_count} defined on table: " <>
              table.name <> " columns:\n" <> inspect(columns)

      # Only support to define one primary key as auto_increment integer
      _pk_count ->
        %{hashids: hashids_count, auto_increment: auto_increment_count} =
          Enum.reduce(columns, %{hashids: 0, auto_increment: 0, none: 0}, fn
            %{type: :hashids}, acc -> Map.update!(acc, :hashids, &(&1 + 1))
            %{auto_increment: true}, acc -> Map.update!(acc, :auto_increment, &(&1 + 1))
            _, acc -> Map.update!(acc, :none, &(&1 + 1))
          end)

        if (total_increment_count = auto_increment_count + hashids_count) > 1 do
          raise MigrationError,
            message:
              "The maximum number of [auto_increment & hashids] pk is 1, but now find #{
                total_increment_count
              } pks defined on table: " <> table.name
        else
          seq_type =
            cond do
              auto_increment_count > 0 -> :self_seq
              hashids_count > 0 -> :default_seq
              true -> :none_seq
            end

          %{table: table, columns: columns, seq_type: seq_type}
        end
    end
  end

  @doc false
  def do_create_table(repo, %{table: table, columns: columns, seq_type: seq_type}) do
    table_name = get_table_name(table, repo.config())
    repo_meta = Ecto.Adapter.lookup_meta(repo)
    instance = repo_meta.instance
    table_names = Runner.list_table_names(instance)

    # check if not exists
    if table_name not in table_names do
      primary_keys = Enum.map(columns, &transform_table_column/1)

      Logger.info(fn -> ">> table: #{table_name}, primary_keys: #{inspect(primary_keys)}" end)

      options = Keyword.put(table.meta, :max_versions, 1)

      case ExAliyunOts.create_table(instance, table_name, primary_keys, options) do
        :ok ->
          result_str = IO.ANSI.format([:green, "ok", :reset])
          Logger.info(fn -> "create table: #{table_name} result: #{result_str}" end)
          :ok

        result ->
          Logger.error(fn -> "create table: #{table_name} result: #{inspect(result)}" end)
          elem(result, 0)
      end
    else
      result_str = IO.ANSI.format([:yellow, "exists", :reset])
      Logger.info(fn -> ">> table: #{table_name} already #{result_str}" end)
      :already_exists
    end
    |> case do
      :ok ->
        create_seq_table_by_type(seq_type, table_name, table_names, repo, instance)

        {table_name, :ok}

      result ->
        {table_name, result}
    end
  end

  def create_seq_table_by_type(:none_seq, _table_name, _table_names, _repo, _instance),
    do: :ignore

  def create_seq_table_by_type(seq_type, table_name, table_names, repo, instance) do
    seq_table_name =
      case seq_type do
        :self_seq -> repo.__adapter__.bound_sequence_table_name(table_name)
        :default_seq -> Sequence.default_table()
      end

    # check if not exists
    if seq_table_name not in table_names do
      Logger.info(fn ->
        ">> auto create table: #{seq_table_name} for table: " <> table_name
      end)

      Sequence.create(instance, seq_table_name)
    else
      :already_exists
    end
  end

  @doc false
  defp get_table_name(table, repo_config) do
    prefix = table.prefix || Keyword.get(repo_config, :migration_default_prefix)

    if prefix do
      prefix <> table.name
    else
      table.name
    end
  end

  defp transform_table_column(%{
         type: type,
         pk_name: field_name,
         partition_key: partition_key?,
         auto_increment: auto_increment?
       }) do
    field_name =
      if is_binary(field_name) do
        field_name
      else
        Atom.to_string(field_name)
      end

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
  defmacro add_pk(column, type, opts \\ []), do: _add_pk(column, type, opts)

  defp _add_pk(column, type, opts)
       when (is_atom(column) or is_binary(column)) and is_list(opts) do
    validate_pk_type!(column, type)

    quote location: :keep do
      %{
        pk_name: unquote(column),
        type: unquote(type),
        partition_key: Keyword.get(unquote(opts), :partition_key, false),
        auto_increment: Keyword.get(unquote(opts), :auto_increment, false)
      }
    end
  end

  defp validate_pk_type!(column, type) do
    if type in [:integer, :string, :binary, :hashids] do
      :ok
    else
      raise ArgumentError,
            "#{inspect(type)} is not a valid primary key type for column: `#{inspect(column)}`, " <>
              "please use an atom as :integer | :string | :binary | :hashids ."
    end
  end
end

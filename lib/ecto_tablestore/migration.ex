defmodule EctoTablestore.Migration do
  @moduledoc """
  Migrations are used to create your tables.

  Support the partition key is autoincrementing based on this library's wrapper, for this usecase,
  we can use the migration to automatically create an another separated table to generate the serial value
  when `:insert` (viz `PutRow`) or `:batch_write` with `:put` option (viz `BatchWriteRow`).

  In practice, we don't create migration files by hand either, we typically use `mix ecto.ots.gen.migration` to
  generate the file with the proper timestamp and then we just fill in its contents:

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


  After we filled the above migration content, you can run the migration above by going to the root of your project
  and typing:

      $ mix ecto.ots.migrate

  Finally, we successfully create the "ecto_ots_test_posts" table, since the above definition added an autoincrementing
  column for the partition key, there will automatically create an "ecto_ots_test_posts_seq" table to generate a serial integer
  for `:post_id` field when insert a new record.
  """

  alias EctoTablestore.Migration.Runner

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
      import EctoTablestore.Migration
      @before_compile EctoTablestore.Migration
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      def __migration__ do
        :ok
      end
    end
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
  defmacro create(object, do: block) do
    expand_create(object, :create, block)
  end

  defp expand_create(object, command, block) do
    quote do
      table = %Table{} = unquote(object)
      Runner.start_command({unquote(command), EctoTablestore.Migration.__prefix__(table)})

      if table.partition_key do
        opts = Runner.repo_config(:migration_primary_key, [])

        opts =
          opts
          |> Keyword.put(:partition_key, true)
          |> Keyword.put(:auto_increment, true)

        {name, opts} = Keyword.pop(opts, :name, :id)
        {type, opts} = Keyword.pop(opts, :type, :integer)

        add(name, type, opts)
      end

      unquote(block)

      Runner.end_command()
      table
    end
  end

  @doc """
  Returns a table struct that can be given to `create/2`.

  Since Tablestore is a NoSQL service, there are up to 4 primary key(s) can be
  added when creation, the first added key is partition key when set `partition_key`
  option as false.

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
      `bigserial` type for primary keys, but can use the `ex_aliyun_ots` lib's wrapper - Sequence
      to implement it; when `false`, a partition key field is not generated on table creation.
    * `:prefix` - the prefix for the table.
    * `:meta` - define the meta information when create table, can see Tablestore's document for details:

      * `:reserved_throughput_write` - reserve the throughtput for write when create table, an integer,
        the default value is 0;
      * `:reserved_throughput_read` - reserve the throughtput for read when create table, an integer,
        the default value is 0;
      * `:time_to_live` - the survival time of the saved data, a.k.a TTL; an integer, unit as second,
        the default value is -1 (permanent preservation);
      * `:deviation_cell_version_in_sec` - maximum version deviation, the default value is 86400
        seconds, which is 1 day;
      * `stream_spec` - set the stream specification of Tablestore:
    
        - `is_enabled`, open or close stream
        - `expiration_time`, the expriration time of the table's stream

  """
  def table(name, opts \\ [])

  def table(name, opts) when is_atom(name) do
    table(Atom.to_string(name), opts)
  end

  def table(name, opts) when is_binary(name) and is_list(opts) do
    struct(%Table{name: name}, opts)
  end

  @doc """
  Adds a primary key when creating a table.

  This function only accepts types as `:string` | `:binary` | `:integer`.

  About `:auto_increment` option:

    * set `:auto_increment` as `true` and its field is primary key of non-partitioned key, there will
    use Tablestore's auto-increment column to process it.

    * set `:auto_increment` as `true` and its field is partition key, there will use `ex_aliyun_ots`'s
    built-in Sequence function, the actual principle behind it is to use the atomic update operation
    though another separate table when generate serial integer, by default there will add an `:id`
    partition key as `:integer` type, the initial value of the sequence is 0, and the increment step is 1.

  Tablestore can only have up to 4 primary keys, meanwhile the first defined primary key is the
  partition key, Please know that the order of the primary key definition will be directly mapped to
  the created table.

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

  ## Options

    * `:partition_key` - when `true`, marks this field as the partition key, only the first explicitly defined field is available for this option.
    * `:auto_increment` - when `true` and this field is non-partitioned key, Tablestore automatically generates the primary key value, which is unique
      in the partition key, and which increases progressively, when `true` and this field is a partition key, use `ex_aliyun_ots`'s Sequence to build
      a serial number for this field, the `auto_increment: true` option only allows binding of one primary key.

  """
  def add(column, type, opts \\ []) when is_atom(column) and is_list(opts) do
    validate_type!(type)
    Runner.subcommand({:add, column, type, opts})
  end

  @doc false
  def __prefix__(%{prefix: prefix} = table) do
    runner_prefix = Runner.prefix()

    cond do
      is_nil(prefix) ->
        prefix = runner_prefix || Runner.repo_config(:migration_default_prefix, nil)
        %{table | prefix: prefix}

      is_nil(runner_prefix) or runner_prefix == to_string(prefix) ->
        table

      true ->
        raise Ecto.MigrationError,
          message:
            "the :prefix option `#{prefix}` does match the migrator prefix `#{runner_prefix}`"
    end
  end

  defp validate_type!(type)
       when type == :integer
       when type == :string
       when type == :binary do
    :ok
  end

  defp validate_type!(type) do
    raise ArgumentError,
          "#{inspect(type)} is not a valid primary key type, " <>
            "please use an atom as :integer | :string | :binary ."
  end
end

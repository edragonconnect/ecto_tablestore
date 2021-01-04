defmodule EctoTablestore.MigrationTest do
  use ExUnit.Case
  import EctoTablestore.Migration, only: [table: 1, table: 2, create: 2, add_pk: 2, add_pk: 3]
  alias EctoTablestore.Migration
  alias Ecto.MigrationError

  @repo EctoTablestore.TestRepo
  @instance EDCEXTestInstance

  setup_all do
    TestHelper.setup_all()
    table_name = "migration_test"

    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(@instance)

    if table_name in table_names do
      ExAliyunOts.delete_table(@instance, table_name)
    end

    on_exit(fn ->
      ExAliyunOts.delete_table(@instance, table_name)
    end)
  end

  # The partition key only can define one
  test "only one partition_key" do
    assert_raise MigrationError,
                 ~r/^The maximum number of partition primary keys is 1, now is 2 defined on table:/,
                 fn ->
                   Migration.__create_table__(table("table_name"), [
                     add_pk(:id1, :integer, partition_key: true),
                     add_pk(:id2, :integer, partition_key: true)
                   ])
                 end
  end

  # The number of primary keys can not be more than 4
  test "only 4 pk" do
    assert_raise MigrationError,
                 ~r/^The maximum number of primary keys is 4, now is 5 defined on table:/,
                 fn ->
                   Migration.__create_table__(table("table_name"), [
                     add_pk(:id1, :integer, partition_key: true),
                     add_pk(:id2, :integer),
                     add_pk(:id3, :integer),
                     add_pk(:id4, :integer),
                     add_pk(:id5, :integer)
                   ])
                 end
  end

  # No partition key defined
  test "at least one pk" do
    assert_raise MigrationError,
                 "Please define at least one partition primary keys for table: table_name",
                 fn ->
                   Migration.__create_table__(table("table_name", partition_key: false), [
                     add_pk(:id1, :integer),
                     add_pk(:id2, :integer),
                     add_pk(:id3, :integer),
                     add_pk(:id4, :integer)
                   ])
                 end
  end

  # Only support to define one primary key as auto_increment integer
  test "more then one [auto_increment & hashids] pk" do
    assert_raise MigrationError,
                 "The maximum number of [auto_increment & hashids] pk is 1, but now find 2 pks defined on table: table_name",
                 fn ->
                   Migration.__create_table__(table("table_name"), [
                     add_pk(:id1, :integer, partition_key: true),
                     add_pk(:id2, :integer, auto_increment: true),
                     add_pk(:id3, :integer, auto_increment: true)
                   ])
                 end

    assert_raise MigrationError,
                 "The maximum number of [auto_increment & hashids] pk is 1, but now find 2 pks defined on table: table_name",
                 fn ->
                   Migration.__create_table__(table("table_name"), [
                     add_pk(:id1, :hashids, partition_key: true, auto_increment: true),
                     add_pk(:id2, :integer, auto_increment: true),
                     add_pk(:id3, :integer)
                   ])
                 end
  end

  # Make the partition key as `:id` and in an increment integer sequence
  test "auto generate partition_key" do
    table = table("table_name")

    runner = setup_runner(@repo)

    assert Migration.__create_table__(table, [
             add_pk(:age, :integer)
           ]) == %{
             table: table,
             columns: [
               add_pk(:id, :integer, auto_increment: true, partition_key: true),
               add_pk(:age, :integer)
             ],
             seq_type: :self_seq
           }

    stop_runner(runner)
  end

  test "self_seq" do
    table = table("table_name")
    runner = setup_runner(@repo)

    columns = [
      add_pk(:id, :integer, auto_increment: true, partition_key: true),
      add_pk(:age, :integer)
    ]

    assert Migration.__create_table__(table, columns) == %{
             table: table,
             columns: columns,
             seq_type: :self_seq
           }

    stop_runner(runner)
  end

  test "default_seq(hashids)" do
    table = table("table_name")

    columns1 = [
      add_pk(:id, :hashids, partition_key: true),
      add_pk(:age, :integer)
    ]

    assert Migration.__create_table__(table, columns1) == %{
             table: table,
             columns: columns1,
             seq_type: :default_seq
           }

    columns2 = [
      add_pk(:id, :hashids, partition_key: true, auto_increment: true),
      add_pk(:age, :integer)
    ]

    assert Migration.__create_table__(table, columns2) == %{
             table: table,
             columns: columns2,
             seq_type: :default_seq
           }
  end

  test "none_seq" do
    table = table("table_name")

    columns = [
      add_pk(:id, :integer, partition_key: true),
      add_pk(:age, :integer),
      add_pk(:name, :string),
      add_pk(:other, :binary)
    ]

    assert Migration.__create_table__(table, columns) == %{
             table: table,
             columns: columns,
             seq_type: :none_seq
           }
  end

  test "create table" do
    table_name = "table_name"
    table = table(table_name)
    runner = setup_runner(@repo)

    create table do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_pk(:other, :binary)
    end

    %{commands: commands} = Agent.get(runner, & &1)
    assert length(commands) == 1
    stop_runner(runner)
  end

  test "create table if not exists" do
    table_name = "migration_test"
    table = table(table_name)
    runner = setup_runner(@repo)

    create table do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_pk(:other, :binary)
    end

    %{commands: commands, repo: repo} = Agent.get(runner, & &1)
    fun = fn -> commands |> Enum.reverse() |> Enum.map(& &1.(repo)) end

    assert fun.() == [{table_name, :ok}]

    stop_runner(runner)
  end

  test "create table if exists" do
    table_name = "migration_test"
    table = table(table_name)
    runner = setup_runner(@repo)

    create table do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_pk(:other, :binary)
    end

    %{commands: commands, repo: repo} = Agent.get(runner, & &1)
    fun = fn -> commands |> Enum.reverse() |> Enum.map(& &1.(repo)) end
    assert fun.() == [{table_name, :already_exists}]

    stop_runner(runner)
  end

  defp setup_runner(repo) do
    args = {self(), repo, __MODULE__, %{level: :debug}}

    {:ok, runner} =
      DynamicSupervisor.start_child(
        EctoTablestore.MigratorSupervisor,
        {EctoTablestore.Migration.Runner, args}
      )

    Process.put(:ecto_tablestore_runner, runner)
    runner
  end

  defp stop_runner(runner) do
    Agent.stop(runner)
  end
end

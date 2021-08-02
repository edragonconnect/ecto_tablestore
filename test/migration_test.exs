defmodule EctoTablestore.MigrationTest do
  use ExUnit.Case
  use EctoTablestore.Migration
  alias EctoTablestore.{Migrator, Migration, Migration.SchemaMigration}
  alias Ecto.MigrationError

  @repo EctoTablestore.TestRepo
  @instance EDCEXTestInstance
  @used_tables [
    EctoTablestore.Sequence.default_table(),
    "migration_test_create_table",
    "migration_test_create_table_id",
    "migration_test_create_table_hashids",
    "migration_test",
    "migration_test1",
    "migration_test2",
    "schema_migrations"
  ]

  setup_all do
    TestHelper.setup_all()
    on_exit(&cleanup_tables/0)
  end

  # The partition key only can define one
  test "only one partition_key" do
    assert_raise MigrationError,
                 ~r/^The maximum number of partition primary keys is 1, now is 2 defined on table:/,
                 fn ->
                   Migration.__create__(table("table_name"), [
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
                   Migration.__create__(table("table_name"), [
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
                   Migration.__create__(table("table_name", partition_key: false), [
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
                 "The maximum number of [auto_increment & hashids] primary keys is 1, but now find 2 primary keys defined on table: table_name",
                 fn ->
                   Migration.__create__(table("table_name"), [
                     add_pk(:id1, :integer, partition_key: true),
                     add_pk(:id2, :integer, auto_increment: true),
                     add_pk(:id3, :integer, auto_increment: true)
                   ])
                 end

    assert_raise MigrationError,
                 "The maximum number of [auto_increment & hashids] primary keys is 1, but now find 2 primary keys defined on table: table_name",
                 fn ->
                   Migration.__create__(table("table_name"), [
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

    columns = [
      add_pk(:id, :integer, auto_increment: true, partition_key: true),
      add_pk(:age, :integer)
    ]

    assert %{
             table: ^table,
             pk_columns: ^columns,
             pre_defined_columns: [],
             create_seq_table?: true
           } = Migration.__create__(table, [add_pk(:age, :integer)])

    stop_runner(runner)
  end

  test "default_seq(id)" do
    table = table("table_name")
    runner = setup_runner(@repo)

    columns_id = [
      add_pk(:id, :id),
      add_pk(:age, :integer)
    ]

    columns = [
      add_pk(:id, :integer, auto_increment: true, partition_key: true),
      add_pk(:age, :integer)
    ]

    assert columns_id == columns

    assert %{
             table: ^table,
             pk_columns: ^columns,
             pre_defined_columns: [],
             create_seq_table?: true
           } = Migration.__create__(table, columns)

    stop_runner(runner)
  end

  test "default_seq(hashids)" do
    table = table("table_name")

    columns1 = [
      add_pk(:id, :hashids, partition_key: true),
      add_pk(:age, :integer)
    ]

    assert %{
             table: ^table,
             pk_columns: ^columns1,
             pre_defined_columns: [],
             create_seq_table?: true
           } = Migration.__create__(table, columns1)

    columns2 = [
      add_pk(:id, :hashids, partition_key: true, auto_increment: true),
      add_pk(:age, :integer)
    ]

    assert %{
             table: ^table,
             pk_columns: ^columns2,
             pre_defined_columns: [],
             create_seq_table?: true
           } = Migration.__create__(table, columns2)
  end

  test "none_seq" do
    table = table("table_name")

    columns = [
      add_pk(:id, :integer, partition_key: true),
      add_pk(:age, :integer),
      add_pk(:name, :string),
      add_pk(:other, :binary)
    ]

    assert %{
             table: ^table,
             pk_columns: ^columns,
             pre_defined_columns: [],
             create_seq_table?: false
           } = Migration.__create__(table, columns)
  end

  test "create table: add pre-defined columns" do
    table = table("table_name")

    pk_columns = [
      add_pk(:id, :integer, partition_key: true),
      add_pk(:age, :integer),
      add_pk(:name, :string),
      add_pk(:other, :binary)
    ]

    pre_defined_columns = [
      add_column(:col1, :integer),
      add_column(:col2, :double),
      add_column(:col3, :boolean),
      add_column(:col4, :string),
      add_column(:col5, :binary)
    ]

    columns = pk_columns ++ pre_defined_columns

    assert %{
             table: ^table,
             pk_columns: ^pk_columns,
             pre_defined_columns: ^pre_defined_columns,
             create_seq_table?: false
           } = Migration.__create__(table, columns)
  end

  test "create table: add secondary index" do
    table = table("table_name")

    pk_columns = [
      add_pk(:id, :integer, partition_key: true),
      add_pk(:age, :integer),
      add_pk(:name, :string),
      add_pk(:other, :binary)
    ]

    pre_defined_columns = [
      add_column(:col1, :integer),
      add_column(:col2, :double),
      add_column(:col3, :boolean),
      add_column(:col4, :string),
      add_column(:col5, :binary)
    ]

    index_metas = [
      add_index("table_name_index1", [:col1, :id], [:col2]),
      add_index("table_name_index2", [:col4, :id], [:col1, :col2, :col3, :col5])
    ]

    columns = pk_columns ++ pre_defined_columns ++ index_metas

    assert %{
             table: ^table,
             pk_columns: ^pk_columns,
             pre_defined_columns: ^pre_defined_columns,
             index_metas: ^index_metas,
             create_seq_table?: false
           } = Migration.__create__(table, columns)
  end

  test "create table" do
    table_name = "migration_test_create_table"
    runner = setup_runner(@repo)

    create table(table_name) do
      add_pk(:id, :integer, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_pk(:other, :binary)
    end

    create table(table_name <> "_id") do
      add_pk(:id, :id)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_pk(:other, :binary)
    end

    create table(table_name <> "_hashids") do
      add_pk(:id, :hashids, partition_key: true)
      add_pk(:age, :integer)
      add_pk(:name, :string)
      add_pk(:other, :binary)
    end

    %{commands: commands, repo: repo} = Agent.get(runner, & &1)
    fun = fn -> commands |> Enum.reverse() |> Enum.map(& &1.(repo)) end
    assert length(commands) == 3

    assert fun.() == [:ok, :ok, :ok]

    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(@instance)

    assert true =
             Enum.all?(
               [
                 EctoTablestore.Sequence.default_table(),
                 table_name,
                 table_name <> "_id",
                 table_name <> "_hashids"
               ],
               &(&1 in table_names)
             )

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

    assert fun.() == [:ok]

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

    assert_raise MigrationError,
                 "create table: migration_test error: Requested table already exists.",
                 fn -> commands |> Enum.reverse() |> Enum.map(& &1.(repo)) end

    stop_runner(runner)
  end

  test "schema_migration: ensure_table" do
    assert :ok = SchemaMigration.ensure_schema_migrations_table!(@repo)
  end

  test "schema_migration: versions" do
    assert [] = SchemaMigration.versions(@repo)
    assert {:ok, _} = SchemaMigration.up(@repo, 1)
    assert [1] = SchemaMigration.versions(@repo)
  end

  test "schema_migration: lock_version" do
    assert old = SchemaMigration.versions(@repo)

    assert_raise MigrationError, "execute failed", fn ->
      SchemaMigration.lock_version!(@repo, 2, fn ->
        {:error, MigrationError.exception("execute failed")}
      end)
    end

    assert :ok = SchemaMigration.lock_version!(@repo, 2, fn -> :ok end)
    assert [2] = SchemaMigration.versions(@repo) -- old

    assert_raise MigrationError,
                 "lock_version failed because of the version: 2 already have",
                 fn ->
                   SchemaMigration.lock_version!(@repo, 2, fn -> :ok end)
                 end

    assert [2] = SchemaMigration.versions(@repo) -- old
  end

  test "with_repo: ensure version or name no duplication" do
    old = SchemaMigration.versions(@repo)
    path = "test/support/migrations/duplicate_name"

    assert_raise MigrationError,
                 "migrations can't be executed, migration name duplicate_name is duplicated",
                 fn ->
                   Migrator.with_repo(@repo, &Migrator.run(&1, path, []), mode: :temporary)
                 end

    path = "test/support/migrations/duplicate_version"

    assert_raise MigrationError,
                 "migrations can't be executed, migration version 3 is duplicated",
                 fn ->
                   Migrator.with_repo(@repo, &Migrator.run(&1, path, []), mode: :temporary)
                 end

    assert ^old = SchemaMigration.versions(@repo)
  end

  test "with_repo: operation function not exported" do
    old = SchemaMigration.versions(@repo)
    path = "test/support/migrations/not_exported"
    module = EctoTablestore.Repo.Migrations.NotExported

    assert_raise MigrationError,
                 "#{inspect(module)} does not implement a `change/0` function",
                 fn ->
                   Migrator.with_repo(@repo, &Migrator.run(&1, path, []), mode: :temporary)
                 end

    assert ^old = SchemaMigration.versions(@repo)
  end

  test "with_repo: filter already executed versions" do
    old = SchemaMigration.versions(@repo)
    path = "test/support/migrations/success"

    assert {:ok, [3, 4], _started} =
             Migrator.with_repo(@repo, &Migrator.run(&1, path, []), mode: :temporary)

    assert old ++ [3, 4] == SchemaMigration.versions(@repo)
    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(@instance)
    assert true = Enum.all?(["migration_test1", "migration_test2"], &(&1 in table_names))
  end

  test "with_repo: drop" do
    old = SchemaMigration.versions(@repo)
    path = "test/support/migrations/drop"

    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(@instance)
    assert true = Enum.all?(["migration_test1", "migration_test2"], &(&1 in table_names))

    assert {:ok, %{index_metas: [_, _]}} =
             ExAliyunOts.describe_table(@instance, "migration_test2")

    assert {:ok, [5], _started} =
             Migrator.with_repo(@repo, &Migrator.run(&1, path, []), mode: :temporary)

    assert old ++ [5] == SchemaMigration.versions(@repo)
    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(@instance)
    assert true = "migration_test1" not in table_names
    assert {:ok, %{index_metas: []}} = ExAliyunOts.describe_table(@instance, "migration_test2")
  end

  test "with_repo: search index" do
    old = SchemaMigration.versions(@repo)
    path = "test/support/migrations/search_index"

    assert {:ok, [6, 7], _started} =
             Migrator.with_repo(@repo, &Migrator.run(&1, path, []), mode: :temporary)

    assert old ++ [6, 7] == SchemaMigration.versions(@repo)
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

  defp cleanup_tables do
    {:ok, %{table_names: table_names}} = ExAliyunOts.list_table(@instance)

    Enum.each(@used_tables, fn table_name ->
      if table_name in table_names do
        ExAliyunOts.delete_table(@instance, table_name)
      end
    end)
  end
end

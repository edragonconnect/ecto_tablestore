defmodule EctoTablestore.TransactionTest do
  use ExUnit.Case
  alias EctoTablestore.TestSchema.TransactionTestRange
  alias EctoTablestore.TestRepo
  alias Ecto.Changeset

  @instance EDCEXTestInstance

  setup_all do
    TestHelper.setup_all()
    EctoTablestore.Support.Table.create_transaction()

    {:ok, item1} =
      TestRepo.insert(%TransactionTestRange{key: "1", key2: 1, field1: "test", status: 1},
        condition: :ignore
      )

    {:ok, item2} =
      TestRepo.insert(%TransactionTestRange{key: "1", key2: 2, field1: "test2", status: 2},
        condition: :ignore
      )

    on_exit(fn ->
      EctoTablestore.Support.Table.delete_transaction()
    end)

    table = TransactionTestRange.__schema__(:source)
    {:ok, table: table, items: [item1, item2]}
  end

  test "transaction/2 function_exported?" do
    assert true == function_exported?(TestRepo, :transaction, 2)
    assert true == function_exported?(TestRepo, :in_transaction?, 0)
    assert true == function_exported?(TestRepo, :rollback, 1)
  end

  test "transaction functions", context do
    transaction_opts = [table: context.table, partition_key: {"key", "1"}]
    # in_transaction?
    assert {:ok, true} == TestRepo.transaction(&TestRepo.in_transaction?/0, transaction_opts)

    # rollback
    assert {:error, "rollback"} ==
             TestRepo.transaction(fn -> TestRepo.rollback("rollback") end, transaction_opts)

    # normal return
    assert {:ok, :any_return} == TestRepo.transaction(fn -> :any_return end, transaction_opts)

    # commit return
    assert {:ok, :any_return} ==
             TestRepo.transaction(fn -> {:commit, :any_return} end, transaction_opts)

    # abort return
    assert {:error, :any_return} ==
             TestRepo.transaction(fn -> {:abort, :any_return} end, transaction_opts)

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))
  end

  test "exit when use transaction", context do
    transaction_opts = [table: context.table, partition_key: {"key", "1"}]

    assert {:error, _} =
             TestRepo.transaction(
               fn -> TestRepo.transaction(fn -> 1 / 0 end, transaction_opts) end,
               transaction_opts
             )

    assert {:error, _} =
             TestRepo.transaction(fn -> 1 / 0 end, transaction_opts)

    assert {:error, _} =
             TestRepo.transaction(fn -> raise "test" end, transaction_opts)

    assert {:error, _} =
             TestRepo.transaction(fn -> throw("test") end, transaction_opts)

    assert {:error, _} =
             TestRepo.transaction(fn -> exit("test") end, transaction_opts)

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))
  end

  test "update & get in transaction", context do
    item = hd(context.items)
    transaction_opts = [table: context.table, partition_key: {"key", "1"}]

    assert {:error, "rollback"} =
             TestRepo.transaction(
               fn ->
                 {:ok, _} =
                   item
                   |> Changeset.change(status: {:increment, 1})
                   |> TestRepo.update(
                     condition: :expect_exist,
                     stale_error_message: "no_exist",
                     stale_error_field: :condition,
                     returning: [:status]
                   )

                 %{status: 2} = TestRepo.one(item)
                 TestRepo.rollback("rollback")
               end,
               transaction_opts
             )

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))

    assert %{status: 1} = TestRepo.one(item)
  end

  test "insert & one in transaction", context do
    transaction_opts = [table: context.table, partition_key: {"key", "3"}]
    TestRepo.delete(%TransactionTestRange{key: "3", key2: 3}, condition: :ignore)

    assert {:error, "rollback"} =
             TestRepo.transaction(
               fn ->
                 {:ok, item} =
                   TestRepo.insert(
                     %TransactionTestRange{key: "3", key2: 3, field1: "test3", status: 3},
                     condition: :expect_not_exist
                   )

                 %{key: "3"} = TestRepo.one(item)
                 TestRepo.rollback("rollback")
               end,
               transaction_opts
             )

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))

    assert match?(nil, TestRepo.one(%TransactionTestRange{key: "3", key2: 3}))

    assert {:ok, item} =
             TestRepo.transaction(
               fn ->
                 {:ok, item} =
                   TestRepo.insert(
                     %TransactionTestRange{key: "3", key2: 3, field1: "test3", status: 3},
                     condition: :expect_not_exist
                   )

                 item
               end,
               transaction_opts
             )

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))

    assert %{key: "3"} = TestRepo.one(item)
    {:ok, _} = TestRepo.delete(item, condition: :expect_exist)
  end

  test "delete in transaction", context do
    item = hd(context.items)
    transaction_opts = [table: context.table, partition_key: {"key", "1"}]

    assert {:error, "rollback"} =
             TestRepo.transaction(
               fn ->
                 {:ok, item} = TestRepo.delete(item, condition: :expect_exist)
                 nil = TestRepo.one(item)
                 TestRepo.rollback("rollback")
               end,
               transaction_opts
             )

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))

    assert %{key: "1"} = TestRepo.one(item)
  end

  test "batch_write & get_range in transaction", context do
    transaction_opts = [table: context.table, partition_key: {"key", "3"}]

    TestRepo.batch_write(
      delete: Enum.map(1..3, &{%TransactionTestRange{key: "3", key2: &1}, condition: :ignore})
    )

    put_list =
      Enum.map(
        1..3,
        &{
          %TransactionTestRange{key: "3", key2: &1, field1: "test3", status: 3},
          condition: :expect_not_exist
        }
      )

    assert {:error, "rollback"} =
             TestRepo.transaction(
               fn ->
                 {:ok, _} = TestRepo.batch_write(put: put_list)

                 {list, nil} =
                   TestRepo.get_range(TransactionTestRange, [{"key", "3"}, {"key2", :inf_min}], [
                     {"key", "3"},
                     {"key2", :inf_max}
                   ])

                 3 = length(list)
                 list = TestRepo.stream(%TransactionTestRange{key: "3"}) |> Enum.to_list()
                 3 = length(list)
                 TestRepo.rollback("rollback")
               end,
               transaction_opts
             )

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))

    assert [] = TestRepo.stream(%TransactionTestRange{key: "3"}) |> Enum.to_list()
  end

  test "commit transaction", context do
    key = "4"
    transaction_opts = [table: context.table, partition_key: {"key", key}]

    delete_list =
      Enum.map(1..10, &{%TransactionTestRange{key: key, key2: &1}, condition: :ignore})

    TestRepo.batch_write(delete: delete_list)

    put_list =
      Enum.map(
        1..10,
        &{
          %TransactionTestRange{key: key, key2: &1, field1: "test4", status: 4},
          condition: :expect_not_exist
        }
      )

    assert {:ok, %{status: 5}} =
             TestRepo.transaction(
               fn ->
                 {:ok, _} = TestRepo.batch_write(put: put_list)

                 {:ok, _} =
                   %TransactionTestRange{key: key, key2: 1, field1: "test4", status: 4}
                   |> Changeset.change(status: {:increment, 1})
                   |> TestRepo.update(
                     condition: :expect_exist,
                     stale_error_message: "no_exist",
                     stale_error_field: :condition,
                     returning: [:status]
                   )

                 {:ok, _} =
                   TestRepo.insert(
                     %TransactionTestRange{key: key, key2: 11, field1: "test4", status: 4},
                     condition: :expect_not_exist
                   )

                 {:ok, _} =
                   TestRepo.delete(%TransactionTestRange{key: key, key2: 11}, condition: :ignore)

                 TestRepo.one(%TransactionTestRange{key: key, key2: 1})
               end,
               transaction_opts
             )

    assert is_nil(Process.get(:current_transaction_id))
    assert is_nil(Process.get(:abort_transaction))

    assert 10 = TestRepo.stream(%TransactionTestRange{key: key}) |> Enum.to_list() |> length()
    assert %{status: 5} = TestRepo.one(%TransactionTestRange{key: key, key2: 1})
    assert %{status: 4} = TestRepo.one(%TransactionTestRange{key: key, key2: 2})
    assert TestRepo.one(%TransactionTestRange{key: key, key2: 11}) |> is_nil()
    TestRepo.batch_write(delete: delete_list)
  end

  test "batch write with transaction id", context do
    {:ok, response} = ExAliyunOts.start_local_transaction(@instance, context.table, {"key", "1"})

    [item1, item2] = context.items
    u1 = {Changeset.change(item1, field1: "updated_test1"), condition: :expect_exist}
    u2 = {Changeset.change(item2, field1: "updated_test2"), condition: :expect_exist}

    transaction_id = response.transaction_id

    assert {:ok, _result} =
             TestRepo.batch_write([update: [u1, u2]], transaction_id: transaction_id)

    ExAliyunOts.abort_transaction(@instance, transaction_id)
    item = TestRepo.one(%TransactionTestRange{key: "1", key2: 1})
    assert item.field1 == "test"
  end
end

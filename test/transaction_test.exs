defmodule EctoTablestore.TransactionTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.TransactionTestRange
  alias EctoTablestore.TestRepo

  @instance EDCEXTestInstance

  import EctoTablestore.Query, only: [condition: 1]

  setup_all do
    TestHelper.setup_all()

    {:ok, item1} =
      TestRepo.insert(
        %TransactionTestRange{key: "1", key2: 1, field1: "test", status: 1},
        condition: condition(:ignore)
      )

    {:ok, item2} =
      TestRepo.insert(
        %TransactionTestRange{key: "1", key2: 2, field1: "test2", status: 2},
        condition: condition(:ignore)
      )

    on_exit(fn ->
      table =  TransactionTestRange.__schema__(:source)

      ExAliyunOts.delete_row(@instance, table, [{"key", "1"}, {"key2", 1}],
        condition: condition(:ignore))

      ExAliyunOts.delete_row(@instance, table, [{"key", "1"}, {"key2", 2}],
        condition: condition(:ignore))
    end)

    {:ok, items: [item1, item2]}
  end

  test "batch write with transaction id", context do

    table = TransactionTestRange.__schema__(:source)
    {:ok, response} = ExAliyunOts.start_local_transaction(@instance, table, {"key", "1"})
    transaction_id = response.transaction_id

    [item1, item2] = context[:items]

    c1 = Ecto.Changeset.change(item1, field1: "updated_test1")
    c2 = Ecto.Changeset.change(item2, field1: "updated_test2")

    {status, _result} =
      TestRepo.batch_write(
        [
          update: [c1, c2]
        ],
        transaction_id: transaction_id
      )

    assert status == :ok

    ExAliyunOts.abort_transaction(@instance, transaction_id)

    item = TestRepo.one(%TransactionTestRange{key: "1", key2: 1})

    assert item.field1 == "test"
  end
end

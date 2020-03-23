defmodule EctoTablestore.AutoIncSequenceTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.Page
  alias EctoTablestore.TestRepo

  import EctoTablestore.Query, only: [condition: 1]

  setup_all do
    TestHelper.setup_all()

    EctoTablestore.Support.Table.create_page()

    on_exit(fn ->
      EctoTablestore.Support.Table.delete_page()
    end)
  end

  test "insert" do
    # before insert operation, there should be run the corrsponding
    # migration to create the table and its seq table
    page = %Page{
      name: "hello page",
      content: "test content"
    }

    {:ok, page} = TestRepo.insert(page, condition: condition(:expect_not_exist), return_type: :pk)
    assert page.pid != nil
  end

  test "batch_insert" do
    {:ok, result} =
      TestRepo.batch_write([
        put: [
          {%Page{name: "test name1", content: "test content1"}, condition: condition(:expect_not_exist), return_type: :pk},
          {%Page{name: "test name2", content: "test content1"}, condition: condition(:expect_not_exist), return_type: :pk},
          {%Page{name: "test name3", content: "test content1"}, condition: condition(:expect_not_exist), return_type: :pk},
          {%Page{name: "test name4", content: "test content1"}, condition: condition(:expect_not_exist), return_type: :pk},
          {%Page{name: "test name5", content: "test content1"}, condition: condition(:expect_not_exist), return_type: :pk},
        ]
      ])
    [{Page, [put: put_results]}] = result
    [{:ok, p1}, {:ok, p2}, {:ok, p3}, {:ok, p4}, {:ok, p5}] = put_results
    assert p1.pid + 1 == p2.pid
    assert p2.pid + 1 == p3.pid
    assert p3.pid + 1 == p4.pid
    assert p4.pid + 1 == p5.pid
  end
end

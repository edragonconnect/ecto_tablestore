defmodule EctoTablestore.AutoIncSequenceTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.Page
  alias EctoTablestore.TestRepo

  import EctoTablestore.Query, only: [condition: 1]

  setup_all do
    TestHelper.setup_all()
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

end

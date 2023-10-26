defmodule EctoTablestore.SQLTest do
  use ExUnit.Case
  require Logger
  alias EctoTablestore.TestRepo
  alias EctoTablestore.TestSchema.Order2
  alias EctoTablestore.Support.SQL, as: TestSQL

  @table "ecto_ots_test_order2"

  setup_all do
    TestHelper.setup_all()

    r1 = TestSQL.setup()
    Logger.info("SQLTest - create table result: #{inspect(r1)}")
    r2 = TestSQL.create_search_index()
    Logger.info("SQLTest - create search_index result: #{inspect(r2)}")
    r3 = TestSQL.create_mapping_table()
    Logger.info("SQLTest - create mapping_table result: #{inspect(r3)}")
    r4 = TestSQL.insert_data()
    Logger.info("SQLTest - insert data result: #{inspect(r4)}")
    Logger.info("SQLTest - waiting index ok...")
    loop_wait_index_ok()
    Logger.info("SQLTest - !! index ok !!")
    on_exit(&TestSQL.cleanup/0)
  end

  defp loop_wait_index_ok do
    Process.sleep(1000)

    with {:ok, rows} <- TestRepo.sql_query(Order2, "SELECT * FROM #{@table} LIMIT 20"),
         11 <- length(rows) do
      :ok
    else
      _ -> loop_wait_index_ok()
    end
  end

  test "normal sql" do
    assert {:ok, rows} = TestRepo.sql_query(Order2, "SELECT * FROM #{@table} LIMIT 20")
    assert 11 = length(rows)
    assert %Order2{id: 1} = hd(rows)

    assert {:ok, rows} = TestRepo.sql_query(Order2, "SELECT * FROM #{@table} WHERE price > 30")

    assert 2 = length(rows)
    assert %Order2{id: 4} = hd(rows)

    assert {:ok, rows} =
             TestRepo.sql_query(Order2, "SELECT * FROM #{@table} WHERE name = 'Banana'")

    assert 3 = length(rows)
    assert %Order2{id: 2} = hd(rows)

    assert {:ok, [%Order2{id: 11}]} =
             TestRepo.sql_query(
               Order2,
               "SELECT * FROM #{@table} WHERE name = 'Banana' AND price > 10"
             )

    assert {:ok, rows} =
             TestRepo.sql_query(Order2, "SELECT * FROM #{@table} ORDER BY price LIMIT 3")

    assert match?([%Order2{id: 7}, %Order2{id: 2}, %Order2{id: 10}], rows)

    assert {:ok, rows} =
             TestRepo.sql_query(Order2, "SELECT * FROM #{@table} ORDER BY price DESC LIMIT 3")

    assert match?([%Order2{id: 4}, %Order2{id: 9}, %Order2{id: 8}], rows)
  end

  test "group by" do
    assert {:ok, rows} =
             TestRepo.sql_query(
               Order2,
               "SELECT name,COUNT(*) as num FROM #{@table} GROUP BY name ORDER BY num DESC LIMIT 3"
             )

    assert match?(
             [
               %Order2{name: "Banana", num: 3},
               %Order2{name: "Orange", num: 2},
               %Order2{name: "Apple", num: 2}
             ],
             rows
           )

    assert {:ok, rows} =
             TestRepo.sql_query(
               Order2,
               "SELECT name,COUNT(*) as num,SUM(price) as price FROM #{@table} GROUP BY name ORDER BY price DESC LIMIT 2"
             )

    assert match?(
             [
               %Order2{name: "Cherry", num: 1, price: 60.8},
               %Order2{name: "Orange", num: 2, price: 45.9}
             ],
             rows
           )

    assert {:ok, rows} =
             TestRepo.sql_query(
               Order2,
               "SELECT name,COUNT(*) as num,SUM(price) as price FROM #{@table} GROUP BY name HAVING price > 60"
             )

    assert match?([%Order2{name: "Cherry", num: 1, price: 60.8}], rows)

    assert {:ok, rows} =
             TestRepo.sql_query(
               Order2,
               "SELECT name,COUNT(*) as num,MAX(price) as price FROM #{@table} GROUP BY name HAVING num > 1 ORDER BY price DESC"
             )

    assert match?(
             [
               %Order2{name: "Orange", num: 2, price: 35.8},
               %Order2{name: "Apple", num: 2, price: 20.8},
               %Order2{name: "Banana", num: 3, price: 12.99}
             ],
             rows
           )
  end

  test "text match" do
    assert {:ok, rows} =
             TestRepo.sql_query(
               Order2,
               "SELECT * FROM #{@table} WHERE TEXT_MATCH(items, 'middle')"
             )

    assert match?([%Order2{id: 1, name: "Apple"}, %Order2{id: 3, name: "Orange"}], rows)

    assert {:ok, rows} =
             TestRepo.sql_query(
               Order2,
               "SELECT * FROM #{@table} WHERE TEXT_MATCH(items, 'middle small')"
             )

    assert match?(
             [
               %Order2{id: 1, name: "Apple"},
               %Order2{id: 3, name: "Orange"},
               %Order2{id: 10, name: "Banana"}
             ],
             rows
           )

    assert {:ok, []} =
             TestRepo.sql_query(
               Order2,
               "SELECT * FROM #{@table} WHERE TEXT_MATCH(items, 'middle small', 'AND')"
             )
  end
end

defmodule EctoTablestore.Support.SQL do
  @moduledoc false

  import EctoTablestore.Query, only: [condition: 1]
  import ExAliyunOts.Search
  alias EctoTablestore.TestRepo
  alias EctoTablestore.TestSchema.Order2

  @instance EDCEXTestInstance
  @table "ecto_ots_test_order2"
  @table_index "ecto_ots_test_order2_index"

  def setup do
    EctoTablestore.Support.Table.create_order2()
  end

  def cleanup do
    ExAliyunOts.delete_search_index(@instance, @table, @table_index)

    with :ok <- ExAliyunOts.drop_mapping_table(@instance, @table),
         :ok <- EctoTablestore.Support.Table.delete_order2() do
    else
      _ ->
        Process.sleep(2000)
        cleanup()
    end
  end

  def create_search_index do
    ExAliyunOts.create_search_index(@instance, @table, @table_index,
      field_schemas: [field_schema_text("items")]
    )
  end

  def create_mapping_table do
    ExAliyunOts.create_mapping_table(@instance, """
    CREATE TABLE ecto_ots_test_order2 (
      id BIGINT PRIMARY KEY,
      name MEDIUMTEXT,
      num BIGINT,
      success BOOL,
      price DOUBLE,
      items MEDIUMTEXT
    );
    """)
  end

  def insert_data do
    data =
      [
        {1, 1, 10.0, "Apple", "Adam", true, ["middle"]},
        {2, 1, 3.6, "Banana", "Allen", true, ["big"]},
        {3, 1, 10.1, "Orange", "Bob", true, ["middle"]},
        {4, 1, 60.8, "Cherry", "Jason", false, []},
        {5, 1, 9.9, "Blackberry", "James", true, []},
        {6, 1, 15.0, "Grape", "Noah", true, []},
        {7, 1, 2.8, "Lemon", "Logan", false, ["big"]},
        {8, 1, 20.8, "Apple", "Amy", true, ["big"]},
        {9, 1, 35.8, "Orange", "Alice", true, ["big"]},
        {10, 1, 6.99, "Banana", "Emma", false, ["small"]},
        {11, 1, 12.99, "Banana", "Isabella", true, ["big"]}
      ]
      |> Enum.map(fn {id, num, price, name, user, success, items} ->
        {%Order2{
           id: id,
           num: num,
           price: price,
           name: name,
           user: user,
           success: success,
           items: items
         }, condition: condition(:expect_not_exist), return_type: :pk}
      end)

    {:ok, _} = TestRepo.batch_write(put: data)
  end
end

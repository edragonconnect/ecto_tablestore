defmodule EctoTablestore.Support.Search do
  require Logger

  alias EctoTablestore.TestSchema.Student

  alias EctoTablestore.TestRepo

  import EctoTablestore.Query, only: [condition: 1]

  import ExAliyunOts.Search,
    only: [
      field_schema_keyword: 1,
      field_schema_integer: 1,
      field_schema_float: 1,
      field_schema_boolean: 1,
      field_schema_nested: 2
    ]

  @instance EDCEXTestInstance

  def initialize(index_names) do
    @instance = EDCEXTestInstance

    table = Student.__schema__(:source)

    create_table(@instance, table)

    create_index(@instance, table, index_names)

    inseart_test_data()
  end

  def clean(useless_index_names) do
    @instance = EDCEXTestInstance
    table = Student.__schema__(:source)

    Enum.map(useless_index_names, fn index_name ->
      {:ok, _response} = ExAliyunOts.delete_search_index(@instance, table, index_name)
    end)

    ExAliyunOts.delete_table(@instance, table)
    Logger.info("clean search_indexes and delete table")
  end

  defp create_table(@instance, table) do
    :ok = ExAliyunOts.create_table(@instance, table, [{"partition_key", :string}])
    Logger.info("initialized table")
    Process.sleep(5_000)
  end

  defp create_index(@instance, table, [index1, index2]) do
    create_search_index(@instance, table, index1)
    create_search_index2(@instance, table, index2)
    Process.sleep(10_000)
  end

  defp inseart_test_data() do
    data = [
      %{
        partition_key: "a1",
        class: "class1",
        name: "name_a1",
        age: 20,
        score: 99.71,
        is_actived: true
      },
      %{
        partition_key: "a2",
        class: "class1",
        name: "name_a2",
        age: 28,
        score: 100.0,
        is_actived: false
      },
      %{
        partition_key: "a3",
        class: "class2",
        name: "name_a3",
        age: 32,
        score: 66.78,
        is_actived: true
      },
      %{
        partition_key: "a4",
        class: "class3",
        name: "name_a4",
        age: 24,
        score: 41.01,
        is_actived: true
      },
      %{
        partition_key: "a5",
        class: "class2",
        name: "name_a5",
        age: 26,
        score: 89.0,
        is_actived: true
      },
      %{
        partition_key: "a6",
        class: "class4",
        name: "name_a6",
        age: 27,
        score: 79.99,
        is_actived: false
      },
      %{
        partition_key: "a7",
        class: "class1",
        name: "name_a7",
        age: 28,
        score: 100.0,
        is_actived: true
      },
      %{
        partition_key: "a8",
        class: "class8",
        name: "name_a8",
        age: 22,
        score: 88.61,
        is_actived: true
      },
      %{
        partition_key: "b9",
        class: "class8",
        name: "name_b9",
        age: 21,
        score: 99.0,
        is_actived: false,
        comment: "comment"
      }
    ]

    Enum.map(data, fn item ->
      Student
      |> struct(item)
      |> TestRepo.insert(condition: condition(:expect_not_exist), return_type: :pk)
    end)

    data = [
      %{partition_key: "a9", content: "[{\"body\":\"body1\",\"header\":\"header1\"}]"},
      %{partition_key: "a10", content: "[{\"body\":\"body2\",\"header\":\"header2\"}]"}
    ]

    Enum.map(data, fn item ->
      Student
      |> struct(item)
      |> TestRepo.insert(condition: condition(:expect_not_exist), return_type: :pk)
    end)

    {:ok, _} =
      ExAliyunOts.update_row(
        @instance,
        Student.__schema__(:source),
        [{"partition_key", "b9"}],
        put: [{"unknown1", "unknownfield1"}, {"unknown2", true}],
        condition: condition(:expect_exist)
      )

    Logger.info("waiting for indexing...")
    Process.sleep(90_000)
  end

  defp create_search_index(@instance, table, index_name) do
    result =
      ExAliyunOts.create_search_index(
        @instance,
        table,
        index_name,
        field_schemas: [
          field_schema_keyword("name"),
          field_schema_integer("age"),
          field_schema_float("score"),
          field_schema_boolean("is_actived"),
          field_schema_keyword("comment")
        ]
      )

    Logger.info("create_search_index: #{inspect(result)}")
  end

  defp create_search_index2(@instance, table, index_name) do
    result =
      ExAliyunOts.create_search_index(
        @instance,
        table,
        index_name,
        field_schemas: [
          field_schema_nested(
            "content",
            field_schemas: [
              field_schema_keyword("header"),
              field_schema_keyword("body")
            ]
          )
        ]
      )

    Logger.info("create_search_index2: #{inspect(result)}")
  end
end

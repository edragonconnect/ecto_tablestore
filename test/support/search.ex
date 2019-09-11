defmodule EctoTablestore.Support.Search do

  require Logger

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.PKType
  alias ExAliyunOts.Const.Search.FieldType
  require PKType
  require FieldType

  alias EctoTablestore.TestSchema.Student
  alias EctoTablestore.TestRepo

  import EctoTablestore.Query, only: [condition: 1]

  def initialize(index_names) do
    instance_key = EDCEXTestInstance
    table = Student.__schema__(:source)

    create_table(instance_key, table)

    create_index(instance_key, table, index_names)

    inseart_test_data()
  end

  def clean(useless_index_names) do
    instance_key = EDCEXTestInstance
    table = Student.__schema__(:source)

    Enum.map(useless_index_names, fn(index_name) ->
      var_request = %Search.DeleteSearchIndexRequest{
        table_name: table,
        index_name: index_name
      }
      {:ok, _response} = Client.delete_search_index(instance_key, var_request)
    end)
    ExAliyunOts.Client.delete_table(instance_key, table)
    Logger.info "clean search_indexes and delete table"
  end

  defp create_table(instance_key, table) do
    var_create_table = %Var.CreateTable{
      table_name: table,
      primary_keys: [{"partition_key", PKType.string}],
    }
    :ok = Client.create_table(instance_key, var_create_table)
    Logger.info "initialized table"
    Process.sleep(5_000)
  end

  defp create_index(instance_key, table, [index1, index2]) do
    create_search_index(instance_key, table, index1)
    create_search_index2(instance_key, table, index2)
    Process.sleep(5_000)
  end

  defp inseart_test_data() do

    data = [
      %{partition_key: "a1", class: "class1", name: "name_a1", age: 20, score: 99.71, is_actived: true},
      %{partition_key: "a2", class: "class1", name: "name_a2", age: 28, score: 100.0, is_actived: false},
      %{partition_key: "a3", class: "class2", name: "name_a3", age: 32, score: 66.78, is_actived: true},
      %{partition_key: "a4", class: "class3", name: "name_a4", age: 24, score: 41.01, is_actived: true},
      %{partition_key: "a5", class: "class2", name: "name_a5", age: 26, score: 89.0, is_actived: true},
      %{partition_key: "a6", class: "class4", name: "name_a6", age: 27, score: 79.99, is_actived: false},
      %{partition_key: "a7", class: "class1", name: "name_a7", age: 28, score: 100.0, is_actived: true},
      %{partition_key: "a8", class: "class8", name: "name_a8", age: 22, score: 88.61, is_actived: true},
      %{partition_key: "b9", class: "class8", name: "name_b9", age: 21, score: 99.0, is_actived: false, comment: "comment"},
    ]

    Enum.map(data, fn(item) ->
      Student
      |> struct(item)
      |> TestRepo.insert(condition: condition(:expect_not_exist), return_type: :pk)
    end)

    data = [
      %{partition_key: "a9", content: "[{\"body\":\"body1\",\"header\":\"header1\"}]"},
      %{partition_key: "a10", content: "[{\"body\":\"body2\",\"header\":\"header2\"}]"},
    ]

    Enum.map(data, fn(item) ->
      Student
      |> struct(item)
      |> TestRepo.insert(condition: condition(:expect_not_exist), return_type: :pk)
    end)

    Logger.info "waiting for indexing..."
    Process.sleep(25_000)
  end

  defp create_search_index(instance_key, table, index_name) do
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "name",
              #field_type: FieldType.keyword, # using as `keyword` field type by default
            },
            %Search.FieldSchema{
              field_name: "age",
              field_type: FieldType.long
            },
            %Search.FieldSchema{
              field_name: "score",
              field_type: FieldType.double
            },
            %Search.FieldSchema{
              field_name: "is_actived",
              field_type: FieldType.boolean
            },
            %Search.FieldSchema{
              field_name: "comment"
            }
          ]
        }
      }
    result = Client.create_search_index(instance_key, var_request)
    Logger.info "create_search_index: #{inspect result}"
  end

  defp create_search_index2(instance_key, table, index_name) do
    sub_nested1 = %Search.FieldSchema{
      field_name: "header",
      field_type: FieldType.keyword,
    }
    sub_nested2 = %Search.FieldSchema{
      field_name: "body",
      field_type: FieldType.keyword,
    }
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "content",
              field_type: FieldType.nested,
              field_schemas: [
                sub_nested1,
                sub_nested2
              ],
            }
          ]
        }
      }
    result = Client.create_search_index(instance_key, var_request)
    Logger.info "create_search_index2: #{inspect result}"
  end

end

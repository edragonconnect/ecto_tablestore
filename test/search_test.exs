defmodule EctoTablestore.SearchTest do
  use ExUnit.Case

  alias EctoTablestore.Support.Search, as: TestSupportSearch

  alias EctoTablestore.TestRepo
  alias EctoTablestore.TestSchema.Student

  import ExAliyunOts.Search

  @indexes ["test_search_index", "test_search_index2"]

  setup_all do
    TestHelper.setup_all()

    TestSupportSearch.initialize(@indexes)

    on_exit(fn ->
      TestSupportSearch.clean(@indexes)
    end)

    :ok
  end

  test "stream_search" do
    index_name = "test_search_index"

    result =
      TestRepo.stream_search(Student, index_name,
        search_query: [
          limit: 3,
          query: prefix_query("name", "n")
        ]
      )
      |> Enum.to_list()

    assert length(result) == 9
  end

  test "match query" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        columns_to_get: ["class", "name"],
        search_query: [
          query: match_query("age", "28"),
          limit: 1
        ]
      )

    assert result.total_hits == 2
    student = List.first(result.schemas)
    assert student.partition_key == "a2"
    assert student.class == "class1"
    assert student.name == "name_a2"
    assert student.age == nil
  end

  test "term query" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query: term_query("age", 28)
        ]
      )

    assert result.total_hits == 2
    assert length(result.schemas) == 2

    for student <- result.schemas do
      assert student.class != nil
      assert is_integer(student.age) == true
      assert student.name != nil
      assert is_float(student.score) == true
    end
  end

  test "term query with sort" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query: terms_query("age", [26, 27, 22]),
          sort: [
            field_sort("age", order: :asc),
            field_sort("name", order: :asc)
          ]
        ]
      )

    assert result.total_hits == 3
    assert length(result.schemas) == 3

    [s1, s2, s3] = result.schemas

    assert s1.age == 22 and s1.name == "name_a8"
    assert s2.age == 26 and s2.name == "name_a5"
    assert s3.age == 27 and s3.name == "name_a6"
  end

  test "prefix query" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query: prefix_query("name", "n"),
          sort: [
            field_sort("age", order: :asc),
            field_sort("name", order: :asc)
          ]
        ]
      )

    assert result.total_hits == 9
    assert length(result.schemas) == 9
  end

  test "wildcard query" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query: wildcard_query("name", "n*"),
          sort: [
            field_sort("age", order: :asc),
            field_sort("name", order: :asc)
          ]
        ],
        columns_to_get: :all
      )

    assert result.total_hits == 9
    assert length(result.schemas) == 9
  end

  test "range query" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query:
            range_query("score",
              from: 60,
              to: 80,
              include_upper: false,
              include_lower: false
            ),
          sort: [
            field_sort("age", order: :desc),
            field_sort("name", order: :asc)
          ]
        ]
      )

    assert result.total_hits == 2

    schema_pks =
      Enum.map(result.schemas, fn schema ->
        schema.partition_key
      end)

    assert schema_pks == ["a3", "a6"]
  end

  test "bool query with must/must_not" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query:
            bool_query(
              must: range_query("age", from: 20, to: 32),
              must_not: term_query("age", 28)
            ),
          sort: [
            field_sort("age", order: :desc),
            field_sort("name", order: :asc)
          ]
        ]
      )

    assert result.total_hits == 7
    assert length(result.schemas) == 7

    ages =
      Enum.map(result.schemas, fn schema ->
        schema.age
      end)

    assert Enum.sort(ages, &(&1 >= &2)) == ages
    assert 28 not in ages
  end

  test "bool query with should" do
    index_name = "test_search_index"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query:
            bool_query(
              should: [
                range_query("age", from: 20, to: 32),
                term_query("score", 66.78)
              ],
              minimum_should_match: 2
            ),
          sort: [
            field_sort("age", order: :desc),
            field_sort("name", order: :asc)
          ]
        ]
      )

    assert result.total_hits == 1
    assert length(result.schemas) == 1
    assert List.first(result.schemas).partition_key == "a3"
  end

  test "nested query" do
    index_name = "test_search_index2"

    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query:
            nested_query(
              "content",
              term_query("content.header", "header1")
            )
        ]
      )

    assert result.total_hits == 1
    assert length(result.schemas) == 1

    [student] = result.schemas

    assert student.partition_key == "a9"
    assert student.content == "[{\"body\":\"body1\",\"header\":\"header1\"}]"
  end

  test "exists query" do
    index_name = "test_search_index"

    # search exists_query for `comment` field
    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query: exists_query("comment")
        ]
      )

    assert length(result.schemas) >= 1

    # seach exists_query for `comment` field as nil column
    {:ok, result} =
      TestRepo.search(Student, index_name,
        search_query: [
          query: bool_query(must_not: exists_query("comment")),
          limit: 100
        ]
      )

    assert result.next_token == nil
    assert length(result.schemas) == 10
  end
end

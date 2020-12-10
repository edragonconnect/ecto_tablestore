defmodule EctoTablestore.AdapterTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.User
  alias Ecto.Adapters.Tablestore

  alias ExAliyunOts.Const.{LogicOperator, FilterType, ComparatorType, RowExistence}

  require FilterType
  require LogicOperator
  require ComparatorType
  require RowExistence

  import EctoTablestore.Query, only: [condition: 2, condition: 1]

  setup_all do
    EctoTablestore.Support.Table.create_user()

    on_exit(fn ->
      EctoTablestore.Support.Table.delete_user()
    end)
  end

  test "generate_filter_options" do
    # primary key is `id`
    # attribute columns are `name`/`level`
    user_name = "testname"
    user_level = 10
    user = %User{id: 1, name: user_name, level: user_level}
    opts = Tablestore.generate_filter_options(user, [])

    filter = Keyword.get(opts, :filter)
    assert filter.type == FilterType.composite_column()
    composite_filter = filter.filter
    assert composite_filter.combinator == LogicOperator.and()
    sub_filters = composite_filter.sub_filters
    assert length(sub_filters) == 2

    for sub_filter <- sub_filters do
      sub_filter_item = sub_filter.filter
      assert sub_filter_item.comparator == ComparatorType.equal()
      assert sub_filter.type == FilterType.single_column()

      case sub_filter_item.column_name do
        "name" ->
          assert sub_filter_item.column_value == user_name

        "level" ->
          assert sub_filter_item.column_value == user_level
      end
    end
  end

  test "generate_filter_options with columns_to_get option" do
    # primary key is `id`
    # attribute columns are `name`/`level`
    user_name = "testname2"
    user_level = 20
    user = %User{id: 2, name: user_name, level: user_level}

    # Since provide some attribute fields in entity, will use
    # them together to combine a full filter implicitly, and when the field which has been
    # used in filter, they should be in `columns_to_get` as well, or
    # the coresponding branch condition of filter will be failed and finally 
    # affect the return result.
    opts = Tablestore.generate_filter_options(user, columns_to_get: ["level"])

    columns_to_get = Keyword.get(opts, :columns_to_get)
    assert "level" in columns_to_get
    assert "name" in columns_to_get

    filter = Keyword.get(opts, :filter)

    assert filter.type == FilterType.composite_column()
    composite_filter = filter.filter

    assert composite_filter.combinator == LogicOperator.and()
    sub_filters = composite_filter.sub_filters

    assert length(sub_filters) == 2
  end

  test "generate_condition_options" do
    # primary key is `id`
    # attribute columns are `name`/`level`
    user_name = "testname2"
    user_level = 20
    user = %User{id: 2, name: user_name, level: user_level}

    opts = Tablestore.generate_condition_options(user, [])

    condition = Keyword.get(opts, :condition)
    assert condition.row_existence == RowExistence.expect_exist()

    column_condition = condition.column_condition
    assert column_condition.type == FilterType.composite_column()

    filter = column_condition.filter
    assert filter.combinator == LogicOperator.and()
    sub_filters = filter.sub_filters

    assert length(sub_filters) == 2

    for sub_filter <- sub_filters do
      sub_filter_item = sub_filter.filter

      assert sub_filter_item.comparator == ComparatorType.equal()
      assert sub_filter.type == FilterType.single_column()

      case sub_filter_item.column_name do
        "name" ->
          assert sub_filter_item.column_value == user_name

        "level" ->
          assert sub_filter_item.column_value == user_level
      end
    end
  end

  test "generate_condition_options with condition option" do
    # primary key is `id`
    # attribute columns are `name`/`level`
    user_name = "testname2"
    user_level = 20
    user = %User{id: 2, name: user_name, level: user_level}

    opts = Tablestore.generate_condition_options(user, condition: condition(:ignore))

    condition = Keyword.get(opts, :condition)
    assert condition.row_existence == RowExistence.expect_exist()

    user1 = %User{id: 2, name: user_name}

    opts =
      Tablestore.generate_condition_options(user1, condition: condition(:ignore, "level" > 10))

    condition = Keyword.get(opts, :condition)
    assert condition.row_existence == RowExistence.expect_exist()

    column_condition = condition.column_condition

    assert column_condition.type == FilterType.composite_column()

    column_condition_filter = column_condition.filter

    assert column_condition_filter.combinator == LogicOperator.and()

    sub_filters = column_condition_filter.sub_filters

    for sub_filter <- sub_filters do
      sub_filter_item = sub_filter.filter

      assert sub_filter.type == FilterType.single_column()

      case sub_filter_item.column_name do
        "name" ->
          assert sub_filter_item.comparator == ComparatorType.equal()
          assert sub_filter_item.column_value == "testname2"

        "level" ->
          assert sub_filter_item.comparator == ComparatorType.greater_than()
          assert sub_filter_item.column_value == 10
      end
    end

    user2 = %User{id: 1}

    opts2 = Tablestore.generate_condition_options(user2, condition: condition(:ignore))

    condition = Keyword.get(opts2, :condition)
    assert condition.row_existence == RowExistence.ignore()

    opts2 =
      Tablestore.generate_condition_options(user2,
        condition: condition(:expect_exist, "level" == 10)
      )

    condition = Keyword.get(opts2, :condition)
    assert condition.row_existence == RowExistence.expect_exist()

    column_condition = condition.column_condition

    assert column_condition.type == FilterType.single_column()

    column_condition_filter = column_condition.filter
    assert column_condition_filter.column_name == "level"
    assert column_condition_filter.column_value == 10
    assert column_condition_filter.comparator == ComparatorType.equal()

    opts2 =
      Tablestore.generate_condition_options(user2,
        condition: condition(:expect_exist, "level" > 10 and "name" == "myname")
      )

    condition = Keyword.get(opts2, :condition)
    assert condition.row_existence == RowExistence.expect_exist()

    column_condition = condition.column_condition

    assert column_condition.type == FilterType.composite_column()
    column_condition_filter = column_condition.filter
    assert column_condition_filter.combinator == LogicOperator.and()

    sub_filters = column_condition_filter.sub_filters

    for sub_filter <- sub_filters do
      sub_filter_item = sub_filter.filter

      assert sub_filter.type == FilterType.single_column()

      case sub_filter_item.column_name do
        "name" ->
          assert sub_filter_item.comparator == ComparatorType.equal()
          assert sub_filter_item.column_value == "myname"

        "level" ->
          assert sub_filter_item.comparator == ComparatorType.greater_than()
          assert sub_filter_item.column_value == 10
      end
    end

    user3 = %User{id: 3}

    opts3 = Tablestore.generate_condition_options(user3, [])

    assert Keyword.get(opts3, :condition) == nil
  end
end

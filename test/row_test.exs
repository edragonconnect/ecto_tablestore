defmodule EctoTablestore.RowTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.{Order, User, User2, User3, User4, EmbedItem}
  alias EctoTablestore.TestRepo
  alias Ecto.Changeset

  import EctoTablestore.Query, only: [condition: 2, condition: 1, filter: 1]

  setup_all do
    TestHelper.setup_all()

    EctoTablestore.Support.Table.create_order()
    EctoTablestore.Support.Table.create_user()
    EctoTablestore.Support.Table.create_user2()
    EctoTablestore.Support.Table.create_user3()
    EctoTablestore.Support.Table.create_user4()

    on_exit(fn ->
      EctoTablestore.Support.Table.delete_order()
      EctoTablestore.Support.Table.delete_user()
      EctoTablestore.Support.Table.delete_user2()
      EctoTablestore.Support.Table.delete_user3()
      EctoTablestore.Support.Table.delete_user4()
    end)

    Process.sleep(3_000)
  end

  test "repo - insert with expect_exist condition" do
    user = %User4{
      id: "not_exist_id1",
      cars: [
        %User4.Car{name: "car1", status: :bar},
        %User4.Car{name: "car2", status: :foo}
      ],
      info: %User4.Info{
        name: "Mike",
        status: :baz,
        money: Decimal.new(1)
      },
      count: 0,
      item: %EmbedItem{name: "item1"}
    }

    assert_raise Ecto.StaleEntryError, fn ->
      TestRepo.insert(user, condition: condition(:expect_exist), return_type: :pk)
    end

    {:ok, returned_user} = TestRepo.insert(user, condition: condition(:expect_not_exist), return_type: :pk)
    assert length(returned_user.cars) == 2

    # override
    user =
      user
      |> Map.put(:count, 1)
      |> Map.put(:cars, [
        %User4.Car{name: "newcar1", status: :baz}
      ])

    updated_cars = [%User4.Car{name: "newcar1", status: :baz}]

    {:ok, returned_user} = TestRepo.insert(user, condition: condition(:expect_exist), return_type: :pk)
    assert returned_user.cars == updated_cars

    user = TestRepo.one(%User4{id: "not_exist_id1"})
    assert user.cars == updated_cars and user.count == 1
  end

  test "repo - embed schema create/read/update/delete" do
    user4 = %User4{
      id: "1",
      cars: [
        %User4.Car{name: "car1", status: :bar},
        %User4.Car{name: "car2", status: :foo}
      ],
      info: %User4.Info{
        name: "Mike",
        status: :baz,
        money: Decimal.new(1)
      },
      count: 0,
      item: %EmbedItem{name: "item1"}
    }

    {status, _inserted_result} =
      TestRepo.insert(user4, condition: condition(:ignore), return_type: :pk)

    assert :ok == status

    one_result = TestRepo.one(user4)
    assert user4 == one_result

    info = %{
      name: "Ben"
    }

    embed_item = %{
      name: "item2"
    }

    changeset =
      user4
      |> Ecto.Changeset.change(count: {:increment, 1})
      |> Ecto.Changeset.put_embed(:info, info)
      |> Ecto.Changeset.put_embed(:item, embed_item)

    {status, updated_result} =
      TestRepo.update(changeset, condition: condition(:expect_exist), return_type: :pk, returning: [:count])

    assert :ok == status
    assert updated_result.count == 1
    assert updated_result.info.__struct__ == User4.Info and
      updated_result.item.__struct__ == EmbedItem

    {status, _updated_result} =
      TestRepo.delete(%User4{id: "1"}, condition: condition(:expect_exist))

    assert :ok == status
  end

  test "repo - insert/get/delete" do
    input_id = "1"
    input_desc = "order_desc"
    input_num = 1
    input_order_name = "order_name"
    input_success = true
    input_price = 100.09

    order = %Order{
      id: input_id,
      name: input_order_name,
      desc: input_desc,
      num: input_num,
      success?: input_success,
      price: input_price
    }

    # Since "Order" table defined an auto increment primary key,
    # and only expect to use `condition: :IGNORE` in server side,
    # we will use `condition: :IGNORE` internally,
    # so here we can omit condition option.
    {status, saved_order} =
      TestRepo.insert(order, return_type: :pk)

    assert status == :ok
    saved_order_internal_id = saved_order.internal_id

    assert saved_order.id == input_id
    # autogenerate primary key return in schema
    assert saved_order_internal_id != nil and is_integer(saved_order_internal_id) == true

    order_with_non_exist_num = %Order{id: input_id, internal_id: saved_order_internal_id, num: 2}
    query_result = TestRepo.one(order_with_non_exist_num, entity_full_match: true)
    assert query_result == nil
    query_result = TestRepo.one(order_with_non_exist_num)
    assert query_result != nil
    query_result = TestRepo.one(%Order{id: "fake", internal_id: 0})
    assert query_result == nil

    # `num` attribute field will be used in condition filter
    order_with_matched_num = %Order{
      id: input_id,
      internal_id: saved_order_internal_id,
      num: input_num
    }

    query_result_by_one = TestRepo.one(order_with_matched_num, entity_full_match: true)

    assert query_result_by_one != nil
    assert query_result_by_one.desc == input_desc
    assert query_result_by_one.num == input_num
    assert query_result_by_one.name == input_order_name
    assert query_result_by_one.success? == input_success
    assert query_result_by_one.price == input_price

    # query and return fields in `columns_to_get`
    query_result2_by_one =
      TestRepo.one(order_with_matched_num,
        columns_to_get: ["num", "desc"],
        entity_full_match: true
      )

    assert query_result2_by_one.desc == input_desc
    assert query_result2_by_one.num == input_num
    assert query_result2_by_one.name == nil
    assert query_result2_by_one.success? == nil
    assert query_result2_by_one.price == nil

    # Optional use case `get/3`
    query_result_by_get = TestRepo.get(Order, id: input_id, internal_id: saved_order_internal_id)
    assert query_result_by_get == query_result_by_one

    query_result2_by_get =
      TestRepo.get(Order, [internal_id: saved_order_internal_id, id: input_id],
        columns_to_get: ["num", "desc"]
      )

    assert query_result2_by_get == query_result2_by_one

    query_result =
      TestRepo.get(Order, [id: input_id, internal_id: saved_order_internal_id],
        filter: filter("num" == 2)
      )

    assert query_result == nil

    query_result =
      TestRepo.get(Order, [id: input_id, internal_id: saved_order_internal_id],
        filter: filter("num" == input_num)
      )

    assert query_result == query_result_by_get

    assert_raise Ecto.ChangeError, fn ->
      TestRepo.delete!(%Order{id: input_id, internal_id: "invalid"})
    end

    assert_raise Ecto.NoPrimaryKeyValueError, fn ->
      TestRepo.delete(%Order{id: input_id})
    end

    order = %Order{internal_id: saved_order_internal_id, id: input_id}

    assert_raise Ecto.StaleEntryError, fn ->
      TestRepo.delete(order, condition: condition(:expect_exist, "num" == "invalid_num"))
    end

    {result, _} = TestRepo.delete(order, condition: condition(:expect_exist, "num" == input_num))
    assert result == :ok
  end

  test "repo - update" do
    input_id = "1001"
    input_desc = "order_desc"
    input_num = 10
    increment = 1
    input_price = 99.9
    order = %Order{id: input_id, desc: input_desc, num: input_num, price: input_price}
    {:ok, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

    assert saved_order.price == input_price

    new_input_num = 100

    changeset =
      Order.test_changeset(saved_order, %{name: "new_name1", num: new_input_num, price: 88.8})

    {:ok, updated_order0} =
      TestRepo.update(changeset, condition: condition(:expect_exist), return_type: :pk)

    # since the above `test_changeset` don't update price
    assert updated_order0.price == input_price
    assert updated_order0.name == "new_name1"
    assert updated_order0.num == 100

    # 1, atom increment `num` field
    # 2, delete `desc` field
    # 3, update `name` field as "new_order"
    updated_order_name = "new_order_name"

    changeset =
      saved_order
      |> Ecto.Changeset.change(num: {:increment, increment}, desc: nil)
      |> Ecto.Changeset.change(name: updated_order_name)

    {:ok, updated_order} =
      TestRepo.update(changeset, condition: condition(:expect_exist), returning: [:num])

    assert updated_order.desc == nil
    assert updated_order.num == new_input_num + increment
    assert updated_order.name == updated_order_name

    order = TestRepo.get(Order, internal_id: saved_order.internal_id, id: input_id)

    assert order.desc == nil
    assert order.num == new_input_num + increment
    assert order.name == updated_order_name

    TestRepo.delete(%Order{id: input_id, internal_id: saved_order.internal_id},
      condition: condition(:expect_exist)
    )
  end

  test "repo - update with timestamps" do
    user = %User{name: "username", level: 10, level2: 0, id: 1}
    {:ok, user} = TestRepo.insert(user, condition: condition(:ignore))

    # Notice:
    # Require to set `returning` option when use `{:increment, number}`.

    assert_raise Ecto.ConstraintError, ~r(Require to set `:level` in the :returning option), fn ->
      changeset = Ecto.Changeset.change(user, level: {:increment, 1}, name: "updated_name")
      TestRepo.update(changeset, condition: condition(:expect_exist))
    end

    {:ok, user} =
      user
      |> Ecto.Changeset.change(level: {:increment, 1}, name: "updated_name")
      |> TestRepo.update(condition: condition(:expect_exist), returning: [:level])

    assert user.level == 11 and user.level2 == 0

    {:ok, user} =
      user
      |> Ecto.Changeset.change(level: {:increment, -1}, level2: {:increment, 1})
      |> TestRepo.update(condition: condition(:expect_exist), returning: [:level, :level2])

    assert user.level == 10 and user.level2 == 1

    {:ok, user} =
      user
      |> Ecto.Changeset.change(level: {:increment, 1}, level2: {:increment, 0})
      |> TestRepo.update(condition: condition(:expect_exist), returning: [:level2, :level])

    assert user.level == 11 and user.level2 == 1

    {:ok, user} =
      user
      |> Ecto.Changeset.change(level: {:increment, 1}, name: "username2")
      |> TestRepo.update(condition: condition(:expect_exist), returning: [:level])

    assert user.level == 12 and user.level2 == 1 and user.name == "username2" and
             user.inserted_at != nil

    naive_dt = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    dt = DateTime.utc_now() |> DateTime.truncate(:second)

    {:ok, user} =
      user
      |> Ecto.Changeset.change(
        level: {:increment, 1},
        level2: 2,
        name: "username3",
        naive_dt: naive_dt,
        dt: dt,
        profile: %{"test" => 1},
        tags: ["A", "B"]
      )
      |> TestRepo.update(condition: condition(:expect_exist), returning: true)

    # Since `returning: true` but the :inserted_at field is not in the changes, it will be replaced as `nil` in the updated user.
    assert user.level == 13 and user.level2 == 2 and user.inserted_at == nil and
             user.name == "username3" and user.inserted_at == nil

    {:ok, user} =
      user
      |> Ecto.Changeset.change(level: {:increment, 1}, name: "username3", tags: ["C", "D"])
      |> TestRepo.update(condition: condition(:expect_exist), returning: [:level, :tags])

    assert user.level == 14 and user.level2 == 2 and user.tags == ["C", "D"]

    TestRepo.delete(user, condition: condition(:expect_exist))

    user = %User{name: "username", level: 1, level2: 1, id: 1}
    {:ok, saved_user} = TestRepo.insert(user, condition: condition(:ignore))

    assert saved_user.updated_at == saved_user.inserted_at
    assert is_integer(saved_user.updated_at)

    user = TestRepo.get(User, id: 1)

    new_name = "username2"

    changeset = Ecto.Changeset.change(user, name: new_name, level: {:increment, 1})

    # Make sure there is a time difference between `updated_at` and `inserted_at`
    Process.sleep(1000)

    {:ok, updated_user} =
      TestRepo.update(changeset, condition: condition(:expect_exist), returning: [:level])

    assert updated_user.level == 2
    assert updated_user.name == new_name
    assert updated_user.updated_at > updated_user.inserted_at

    TestRepo.delete(user, condition: condition(:expect_exist))
  end

  test "repo - insert/update with ecto types" do
    assert_raise Ecto.StaleEntryError, ~r/attempted to insert a stale struct/, fn ->
      user = %User{
        name: "username2",
        profile: %{"level" => 1, "age" => 20},
        tags: ["tag_a", "tag_b"],
        id: 2
      }

      TestRepo.insert(user, condition: condition(:expect_exist))
    end

    naive_dt = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    dt = DateTime.utc_now() |> DateTime.truncate(:second)

    user = %User{
      id: 1,
      name: "username",
      naive_dt: naive_dt,
      dt: dt,
      profile: %{"level" => 1, "age" => 20},
      tags: ["tag_a", "tag_b"]
    }

    {:ok, _saved_user} = TestRepo.insert(user, condition: condition(:ignore))
    get_user = TestRepo.get(User, id: 1)
    profile = get_user.profile

    assert get_user.naive_dt == naive_dt
    assert get_user.dt == dt
    assert Map.get(profile, "level") == 1
    assert Map.get(profile, "age") == 20
    assert get_user.tags == ["tag_a", "tag_b"]

    naive_dt = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    dt = DateTime.utc_now() |> DateTime.truncate(:second)

    changeset =
      Ecto.Changeset.change(get_user,
        name: "username2",
        profile: %{name: 1},
        naive_dt: naive_dt,
        dt: dt
      )

    {:ok, updated_user} = TestRepo.update(changeset, condition: condition(:expect_exist))

    assert updated_user.naive_dt == naive_dt
    assert updated_user.dt == dt
    assert updated_user.profile == %{name: 1}

    get_user = TestRepo.get(User, id: 1)

    # Please notice that Jason.decode use :key option as :strings by default, we don't
    # provide a way to modify this option so far.
    assert get_user.profile == %{"name" => 1}

    TestRepo.delete(user, condition: condition(:expect_exist))
  end

  describe "range" do
    setup do
      saved_orders =
        Enum.map(1..9, fn var ->
          {:ok, saved_order} =
            TestRepo.insert(%Order{id: "#{var}", desc: "desc#{var}", num: var, price: 20.5 * var},
              condition: condition(:ignore),
              return_type: :pk
            )

          saved_order
        end)

      # double A
      {:ok, order_10_1} =
        TestRepo.insert(%Order{id: "A", desc: "descA", num: 10, price: 20.5 * 10},
          condition: condition(:ignore),
          return_type: :pk
        )

      {:ok, order_10_2} =
        TestRepo.insert(%Order{id: "A", desc: "descA", num: 10, price: 20.5 * 10},
          condition: condition(:ignore),
          return_type: :pk
        )

      saved_orders = saved_orders ++ [order_10_1, order_10_2]

      on_exit(fn ->
        for order <- saved_orders do
          TestRepo.delete(%Order{id: order.id, internal_id: order.internal_id},
            condition: condition(:expect_exist)
          )
        end
      end)
    end

    test "repo - get_range/1-2" do
      assert {nil, nil} = TestRepo.get_range(%Order{id: "0"})

      assert {orders, nil} = TestRepo.get_range(%Order{id: "1"})
      assert length(orders) == 1

      assert {orders, nil} = TestRepo.get_range(%Order{id: "A"})
      assert length(orders) == 2

      assert {orders, nil} = TestRepo.get_range(Order)
      assert length(orders) == 11

      assert {backward_orders, nil} = TestRepo.get_range(Order, direction: :backward)
      assert orders == Enum.reverse(backward_orders)

      assert {limit_orders, next_token} = TestRepo.get_range(Order, limit: 3)
      assert length(limit_orders) == 3
      assert is_binary(next_token)
    end

    test "repo - get_range/3-4" do
      start_pks = [{"id", "0a"}, {"internal_id", :inf_min}]
      end_pks = [{"id", "0b"}, {"internal_id", :inf_max}]

      {orders, next_start_primary_key} = TestRepo.get_range(Order, start_pks, end_pks)
      assert orders == nil and next_start_primary_key == nil

      start_pks = [{"id", "1"}, {"internal_id", :inf_min}, {"id", "1"}]
      end_pks = [{"id", "3"}, {"internal_id", :inf_max}, {"id", "3"}]
      assert {orders, nil} = TestRepo.get_range(Order, start_pks, end_pks)
      assert length(orders) == 3

      start_pks = [{"id", "3"}, {"internal_id", :inf_max}]
      end_pks = [{"id", "1"}, {"internal_id", :inf_min}]

      {backward_orders, _next_start_primary_key} =
        TestRepo.get_range(Order, start_pks, end_pks, direction: :backward)

      assert orders == Enum.reverse(backward_orders)

      start_pks = [{"id", "7"}, {"internal_id", :inf_max}]
      end_pks = [{"id", "4"}, {"internal_id", :inf_min}]

      {orders, next_start_primary_key} =
        TestRepo.get_range(Order, start_pks, end_pks, limit: 3, direction: :backward)

      assert next_start_primary_key != nil
      assert length(orders) == 3

      {orders2, next_start_primary_key} =
        TestRepo.get_range(Order, next_start_primary_key, end_pks, limit: 3, direction: :backward)

      assert next_start_primary_key == nil
      assert length(orders2) == 1
    end

    test "repo - stream" do
      # since it is enumerable, no matched data will return `[]`.
      assert [] =
               %Order{id: "0"}
               |> TestRepo.stream()
               |> Enum.to_list()

      assert 1 =
               %Order{id: "1"}
               |> TestRepo.stream()
               |> Enum.count()

      assert 2 =
               %Order{id: "A"}
               |> TestRepo.stream()
               |> Enum.count()

      orders = TestRepo.stream(Order) |> Enum.to_list()
      assert 11 = Enum.count(orders)

      assert ^orders =
               Order
               |> TestRepo.stream(direction: :backward)
               |> Enum.reverse()

      assert 5 =
               Order
               |> TestRepo.stream(limit: 3)
               |> Enum.take(5)
               |> Enum.count()
    end

    test "repo - stream_range" do
      start_pks = [{"id", "0a"}, {"internal_id", :inf_min}]
      end_pks = [{"id", "0b"}, {"internal_id", :inf_max}]

      # since it is enumerable, no matched data will return `[]`.
      assert [] =
               Order
               |> TestRepo.stream_range(start_pks, end_pks, direction: :forward)
               |> Enum.to_list()

      start_pks = [{"id", "1"}, {"internal_id", :inf_min}]
      end_pks = [{"id", "3"}, {"internal_id", :inf_max}]

      orders =
        Order
        |> TestRepo.stream_range(start_pks, end_pks, direction: :forward, limit: 1)
        |> Enum.to_list()

      assert length(orders) == 3

      # start/end pks with an invalid `direction`
      [{:error, error}] =
        Order
        |> TestRepo.stream_range(start_pks, end_pks, direction: :backward)
        |> Enum.to_list()

      assert error.code == "OTSParameterInvalid" and
               error.message == "Begin key must more than end key in BACKWARD"

      start_pks = [{"id", "3"}, {"internal_id", :inf_max}]
      end_pks = [{"id", "1"}, {"internal_id", :inf_min}]

      assert ^orders =
               Order
               |> TestRepo.stream_range(start_pks, end_pks, direction: :backward)
               |> Enum.reverse()

      start_pks = [{"id", "1"}, {"internal_id", :inf_min}]
      end_pks = [{"id", "9"}, {"internal_id", :inf_max}]

      assert 9 =
               Order
               |> TestRepo.stream_range(start_pks, end_pks, limit: 3)
               |> Enum.count()

      assert 5 =
               Order
               |> TestRepo.stream_range(start_pks, end_pks, limit: 3)
               |> Enum.take(5)
               |> Enum.count()
    end
  end

  test "repo - batch_get" do
    {saved_orders, saved_users} =
      Enum.reduce(1..3, {[], []}, fn var, {cur_orders, cur_users} ->
        order = %Order{id: "#{var}", desc: "desc#{var}", num: var, price: 1.8 * var}

        {:ok, saved_order} =
          TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

        user = %User{id: var, name: "name#{var}", level: var}

        {:ok, saved_user} =
          TestRepo.insert(user, condition: condition(:expect_not_exist), return_type: :pk)

        {cur_orders ++ [saved_order], cur_users ++ [saved_user]}
      end)

    requests1 = [
      {Order, [[{"id", "1"}, {"internal_id", List.first(saved_orders).internal_id}]],
       columns_to_get: ["num"]},
      [%User{id: 1, name: "name1"}, %User{id: 2, name: "name2"}]
    ]

    {:ok, result} = TestRepo.batch_get(requests1)

    [{Order, query_orders}, {User, query_users}] = result

    assert length(query_orders) == 1

    assert length(query_users) == 2

    for query_user <- query_users do
      assert query_user.level != nil
    end

    # provide attribute column `name` in schema, and set `entity_full_match: true` will use these attribute field(s) in the filter and add `name` into columns_to_get if specially set columns_to_get.
    requests2 = [
      {User,
       [
         [{"id", 1}],
         [{"id", 2}]
       ]}
    ]

    {:ok, result2} = TestRepo.batch_get(requests2)
    query_users2 = Keyword.get(result2, User)
    assert length(query_users2) == 2

    requests2 = [
      {User,
       [
         [{"id", 1}],
         [{"id", 2}]
       ], columns_to_get: ["level"]}
    ]

    {:ok, result2} = TestRepo.batch_get(requests2)
    query_users2 = Keyword.get(result2, User)
    assert length(query_users2) == 2

    for query_user <- query_users2 do
      assert query_user.level != nil
    end

    requests2 = [
      {[%User{id: 1, name: "name1"}, %User{id: 2, name: "name2"}],
       columns_to_get: ["level"], entity_full_match: true}
    ]

    {:ok, result2} = TestRepo.batch_get(requests2)

    query_users2 = Keyword.get(result2, User)

    assert length(query_users2) == 2

    for query_user <- query_users2 do
      assert query_user.name != nil
      assert query_user.level != nil
    end

    requests_with_fake = [
      {[%User{id: 1, name: "name_fake"}, %User{id: 2, name: "name2"}],
       columns_to_get: ["level"], entity_full_match: true}
    ]

    {:ok, [{_, result_users}]} = TestRepo.batch_get(requests_with_fake)
    assert length(result_users) == 1

    assert_raise RuntimeError, fn ->
      # When use `entity_full_match: true`, one schema provides `name` attribute,
      # another schema only has primary key,
      # in this case, there exist conflict will raise a RuntimeError for
      requests_invalid = [
        {[%User{id: 1, name: "name1"}, %User{id: 2}], entity_full_match: true}
      ]

      TestRepo.batch_get(requests_invalid)
    end

    # The filter of following case is ((name == "name1" and level == 2) or (name == "name2" and level == 2)
    requests3 = [
      {[%User{id: 1, name: "name1"}, %User{id: 2, name: "name2"}], filter: filter("level" == 2)}
    ]

    {:ok, result3} = TestRepo.batch_get(requests3)

    query_users3 = Keyword.get(result3, User)

    assert length(query_users3) == 1
    query3_user = List.first(query_users3)
    assert query3_user.id == 2
    assert query3_user.name == "name2"

    changeset = Ecto.Changeset.change(%User{id: 1}, name: "new_name1")
    TestRepo.update(changeset, condition: condition(:expect_exist))

    # After update User(id: 1)'s name as `new_name1`, in the next batch get, attribute columns will be used in filter.
    #
    # The following case will only return User(id: 2) in batch get.
    requests3_1 = [
      {[%User{id: 1, name: "name1"}, %User{id: 2, name: "name2"}], entity_full_match: true}
    ]

    {:ok, result3_1} = TestRepo.batch_get(requests3_1)

    query_result3_1 = Keyword.get(result3_1, User)

    assert length(query_result3_1) == 1
    query3_1_user = List.first(query_result3_1)
    assert query3_1_user.id == 2
    assert query3_1_user.name == "name2"

    # Although User(id: 1)'s name is changed, but by default `batch_get` only use the primary keys of User entity to fetch rows.
    requests4 = [
      [%User{id: 1, name: "not_existed_name1"}]
    ]

    {:ok, result4} = TestRepo.batch_get(requests4)
    assert Keyword.get(result4, User) != nil

    for order <- saved_orders do
      TestRepo.delete(%Order{id: order.id, internal_id: order.internal_id},
        condition: condition(:expect_exist)
      )
    end

    for user <- saved_users do
      TestRepo.delete(%User{id: user.id}, condition: condition(:expect_exist))
    end
  end

  test "repo - batch_write" do
    order0 = %Order{id: "order0", desc: "desc0"}
    {:ok, saved_order0} = TestRepo.insert(order0, condition: condition(:ignore), return_type: :pk)

    order1_num = 10
    order1 = %Order{id: "order1", desc: "desc1", num: order1_num, price: 89.1}
    {:ok, saved_order1} = TestRepo.insert(order1, condition: condition(:ignore), return_type: :pk)

    order2 = %Order{id: "order2", desc: "desc2", num: 5, price: 76.6}
    {:ok, saved_order2} = TestRepo.insert(order2, condition: condition(:ignore), return_type: :pk)

    order3 = %Order{id: "order3", desc: "desc3", num: 10, price: 55.67}

    order4_changeset = Changeset.cast(%Order{id: "order4_1", desc: "desc3"}, %{num: 40}, [:num])

    order5 = %Order{id: "order5", desc: "desc5", num: 20, price: 50.11}

    user1_lv = 8

    user1 = %User{id: 100, name: "u1", level: user1_lv}
    {:ok, _} = TestRepo.insert(user1, condition: condition(:expect_not_exist))

    user2 = %User{id: 101, name: "u2", level: 11}
    {:ok, _} = TestRepo.insert(user2, condition: condition(:expect_not_exist))

    user3 = %User{id: 102, name: "u3", level: 12}

    changeset_order1 =
      Order
      |> TestRepo.get(id: "order1", internal_id: saved_order1.internal_id)
      |> Changeset.change(num: {:increment, 1}, price: nil)

    changeset_user1 =
      User
      |> TestRepo.get(id: 100)
      |> Changeset.change(level: {:increment, 1}, name: "new_user_2")

    writes = [
      delete: [
        {saved_order2, entity_full_match: true},
        {Order, [id: "order0", internal_id: saved_order0.internal_id],
         condition: condition(:ignore)},
        {user2, condition: condition(:expect_exist), return_type: :pk}
      ],
      update: [
        {changeset_user1, entity_full_match: true, return_type: :pk},
        {changeset_order1, condition: condition(:expect_exist), return_type: :pk}
      ],
      put: [
        # Since "Order" table defined autogenerate_id field to server auto increment,
        # in this case only can use `condition: :ignore` to the server side logic,
        # and the `entity_full_match: true` setting will be ignored.
        {order3, return_type: :pk},
        {order4_changeset, condition: condition(:ignore), return_type: :pk},
        {order5, entity_full_match: true, return_type: :pk},
        {user3, condition: condition(:expect_not_exist), return_type: :pk},
        {User, [id: 1001], [name: "new_u1", level: 1], condition: condition(:expect_not_exist)}
      ]
    ]

    {:ok, result} = TestRepo.batch_write(writes)

    order_batch_write_result = Keyword.get(result, Order)

    {:ok, batch_write_update_order} =
      Keyword.get(order_batch_write_result, :update) |> List.first()

    assert batch_write_update_order.num == order1_num + 1
    assert batch_write_update_order.id == order1.id
    assert batch_write_update_order.internal_id == saved_order1.internal_id
    assert batch_write_update_order.price == nil

    [{:ok, batch_write_delete_order2}, {:ok, batch_write_delete_order0}] =
      Keyword.get(order_batch_write_result, :delete)

    assert batch_write_delete_order2.id == saved_order2.id
    assert batch_write_delete_order2.internal_id == saved_order2.internal_id

    assert batch_write_delete_order0.id == saved_order0.id
    assert batch_write_delete_order0.internal_id == saved_order0.internal_id

    [
      {:ok, batch_write_put_order3},
      {:ok, batch_write_put_order4},
      {:ok, batch_write_put_order5},
    ] = Keyword.get(order_batch_write_result, :put)

    assert batch_write_put_order3.id == order3.id
    assert batch_write_put_order3.desc == order3.desc
    assert batch_write_put_order3.num == order3.num
    assert batch_write_put_order3.price == order3.price

    assert batch_write_put_order4.id == order4_changeset.data.id
    assert is_integer(batch_write_put_order4.internal_id) == true
    assert batch_write_put_order4.num == order4_changeset.changes.num

    assert batch_write_put_order5.id == order5.id
    assert batch_write_put_order5.desc == order5.desc
    assert batch_write_put_order5.num == order5.num
    assert batch_write_put_order5.price == order5.price

    user_batch_write_result = Keyword.get(result, User)

    {:ok, batch_write_update_user} = Keyword.get(user_batch_write_result, :update) |> List.first()
    assert batch_write_update_user.level == user1_lv + 1
    assert batch_write_update_user.name == "new_user_2"
    assert batch_write_update_user.id == 100

    {:ok, batch_write_delete_user} = Keyword.get(user_batch_write_result, :delete) |> List.first()
    assert batch_write_delete_user.level == 11
    assert batch_write_delete_user.id == 101
    assert batch_write_delete_user.name == "u2"

    batch_write_put = Keyword.get(user_batch_write_result, :put)

    {:ok, batch_write_put_user} = batch_write_put |> List.first()
    assert batch_write_put_user.level == 12
    assert batch_write_put_user.id == 102
    assert batch_write_put_user.name == "u3"

    {:ok, batch_write_put_user1001} = batch_write_put |> List.last()
    assert batch_write_put_user1001.level == 1
    assert batch_write_put_user1001.id == 1001
    assert batch_write_put_user1001.name == "new_u1"

    changeset_user3 = Changeset.change(batch_write_put_user, level: {:increment, 2})

    # failed case
    writes2 = [
      delete: [
        {batch_write_put_user, condition: condition(:expect_exist)},
      ],
      update: [
        {changeset_user3, condition: condition(:expect_exist)}
      ]
    ]

    {:ok, result2} = TestRepo.batch_write(writes2)

    fail_batch_write_result = Keyword.get(result2, User)

    {:error, batch_write_update_response} =
      Keyword.get(fail_batch_write_result, :update) |> List.first()

    assert batch_write_update_response.is_ok == false

    {:error, batch_write_delete_response} =
      Keyword.get(fail_batch_write_result, :delete) |> List.first()

    assert batch_write_delete_response.is_ok == false

    {:ok, _} = TestRepo.delete(batch_write_put_user, condition: condition(:expect_exist))
    {:ok, _} = TestRepo.delete(batch_write_update_user, condition: condition(:expect_exist))
    {:ok, _} = TestRepo.delete(batch_write_put_order3, condition: condition(:ignore))
    {:ok, _} = TestRepo.delete(batch_write_put_order5, condition: condition(:ignore))
    {:ok, _} = TestRepo.delete(batch_write_update_order, condition: condition(:ignore))
  end

  test "repo - batch_update with timestamps" do
    inserted_users =
      for index <- 1..3 do
        u = %User{id: index, name: "u#{index}", level: index}
        {:ok, inserted_user} = TestRepo.insert(u, condition: condition(:expect_not_exist))
        assert inserted_user.inserted_at == inserted_user.updated_at
        inserted_user
      end

    user1 = List.first(inserted_users)

    Process.sleep(1000)

    new_user1_name = "new_u1"

    changeset1 = Changeset.change(user1, name: new_user1_name, level: nil)

    changeset2 = Changeset.change(%User{id: 2}, level: {:increment, 1})

    writes = [
      update: [
        {changeset1, condition: condition(:expect_exist)},
        {changeset2, condition: condition(:expect_exist)}
      ],
      put: [
        {%User{id: 10, name: "new10", level: 10}, condition: condition(:expect_not_exist)},
        {%User{id: 11, name: "new11", level: 11}, condition: condition(:expect_not_exist)}
      ]
    ]

    {:ok, result} = TestRepo.batch_write(writes)

    user_writes = Keyword.get(result, User)
    put_opers = Keyword.get(user_writes, :put)

    for {:ok, put_user} <- put_opers do
      assert put_user.inserted_at == put_user.updated_at
    end

    update_opers = Keyword.get(user_writes, :update)

    for {:ok, update_user} <- update_opers do
      case update_user.id do
        1 ->
          update_updated_at = update_user.updated_at
          update_inserted_at = update_user.inserted_at
          user1_inserted_at = user1.inserted_at

          assert update_updated_at > user1_inserted_at and update_updated_at > update_inserted_at and
                   update_inserted_at == user1_inserted_at

          assert update_user.level == nil
          assert update_user.name == new_user1_name

        2 ->
          updated_at = update_user.updated_at
          assert updated_at != nil and is_integer(updated_at)
          assert update_user.level == 3
          assert update_user.name == nil
      end
    end

    TestRepo.delete(%User{id: 1}, condition: condition(:expect_exist))
    TestRepo.delete(%User{id: 2}, condition: condition(:expect_exist))
    TestRepo.delete(%User{id: 3}, condition: condition(:expect_exist))
    TestRepo.delete(%User{id: 10}, condition: condition(:expect_exist))
    TestRepo.delete(%User{id: 11}, condition: condition(:expect_exist))
  end

  test "repo - at least one attribute column" do
    u = %User3{id: "1"}

    assert_raise Ecto.ConstraintError, fn ->
      TestRepo.insert(u, condition: condition(:expect_not_exist))
    end
  end

  test "repo - get/one/get_range/batch_get with not matched filter" do
    input_id = "10001"
    input_desc = "order_desc"
    input_num = 1
    input_order_name = "order_name"
    input_success = true
    input_price = 100.09

    order = %Order{
      id: input_id,
      name: input_order_name,
      desc: input_desc,
      num: input_num,
      success?: input_success,
      price: input_price
    }

    {:ok, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

    start_pks = [{"id", input_id}, {"internal_id", :inf_min}]
    end_pks = [{"id", "100010"}, {"internal_id", :inf_max}]

    get_result =
      TestRepo.get(Order, [id: input_id, internal_id: saved_order.internal_id],
        filter: filter("num" == 1000)
      )

    assert get_result == nil

    one_result =
      TestRepo.one(%Order{id: input_id, internal_id: saved_order.internal_id},
        filter: filter("num" > 10)
      )

    assert one_result == nil

    {records, next} = TestRepo.get_range(Order, start_pks, end_pks, filter: filter("num" == 100))
    assert records == nil and next == nil

    requests = [
      {Order, [{"id", input_id}, {"internal_id", saved_order.internal_id}],
       filter: filter("num" == 100)}
    ]

    {:ok, [{Order, batch_get_result}]} = TestRepo.batch_get(requests)

    assert batch_get_result == nil

    TestRepo.delete(%Order{id: input_id, internal_id: saved_order.internal_id},
      condition: condition(:expect_exist)
    )
  end

  test "repo - insert/batch_write:put with changeset" do
    id = "1001"
    changeset = Order.test_changeset(%Order{}, %{id: id, name: "test_name", num: 100})
    {:ok, new_order} = TestRepo.insert(changeset, condition: condition(:ignore))
    assert new_order.id == id and new_order.name == "test_name" and new_order.num == 100

    id2 = "1002"
    changeset2 = Order.test_changeset(%Order{}, %{id: id2, name: "test_name2", num: 102})
    id3 = "1003"
    changeset3 = Order.test_changeset(%Order{}, %{id: id3, name: "test_name2", num: 102})

    writes = [
      put: [
        {changeset2, condition: condition(:ignore), return_type: :pk},
        {changeset3, condition: condition(:ignore)}
      ]
    ]

    {:ok, batch_writes_result} = TestRepo.batch_write(writes)

    [{Order, [put: result_items]}] = batch_writes_result

    [{:ok, result1}, {:ok, result2}] = result_items

    assert result1.internal_id != nil and result1.id == "1002"
    # since the second item does not require return pk
    assert result2.internal_id == nil and result2.id == "1003"

    id4 = "1004"
    changeset4 = Order.test_changeset(%Order{}, %{id: id4, name: "test_name2"})

    {:error, changeset} = TestRepo.insert(changeset4, condition: condition(:ignore))

    assert changeset.valid? == false

    writes = [
      put: [
        {changeset4, condition: condition(:ignore), return_type: :pk}
      ]
    ]

    assert_raise RuntimeError, ~r/Using invalid changeset/, fn ->
      TestRepo.batch_write(writes)
    end

    start_pks = [{"id", "1001"}, {"internal_id", :inf_min}]
    end_pks = [{"id", "1003"}, {"internal_id", :inf_max}]

    {orders, _next} = TestRepo.get_range(Order, start_pks, end_pks)

    Enum.map(orders, fn order ->
      TestRepo.delete(order, condition: condition(:expect_exist))
    end)
  end

  test "repo - check insert stale_error" do
    user = %User3{id: "check_insert_stale", name: "name"}
    {:ok, _} = TestRepo.insert(user, condition: condition(:ignore))

    stale_error_field = :id
    stale_error_message = "Row already exists"

    {:error, invalid_changeset} =
      TestRepo.insert(user,
        condition: condition(:expect_not_exist),
        stale_error_field: stale_error_field,
        stale_error_message: stale_error_message
      )

    assert {^stale_error_message, [stale: true]} =
             Keyword.get(invalid_changeset.errors, stale_error_field)
  end

  test "repo - check stale_error" do
    input_id = "10001"
    input_desc = "order_desc"
    input_num = 10
    increment = 1
    input_price = 99.9
    order = %Order{id: input_id, desc: input_desc, num: input_num, price: input_price}
    {:ok, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

    changeset =
      %Order{internal_id: saved_order.internal_id, id: saved_order.id}
      |> Ecto.Changeset.change(num: {:increment, increment}, desc: nil)

    # `stale_error_field` can be any value of atom.
    stale_error_field = :num
    stale_error_message = "check num condition failed"

    {:error, invalid_changeset} =
      TestRepo.update(changeset,
        condition: condition(:expect_exist, "num" > 1000),
        stale_error_field: stale_error_field,
        stale_error_message: stale_error_message,
        returning: [:num]
      )

    {^stale_error_message, error} = Keyword.get(invalid_changeset.errors, stale_error_field)
    assert error == [stale: true]

    {:ok, _} = TestRepo.delete(saved_order, condition: condition(:expect_exist))
  end

  test "repo - naive_datetime timestamp" do
    id = Ecto.UUID.generate()
    {:ok, user} = %User2{id: id} |> TestRepo.insert(condition: condition(:ignore))
    assert NaiveDateTime.compare(NaiveDateTime.utc_now(), user.inserted_at) == :gt
  end

  test "repo batch write to delete with an array field" do
    user1 = %User{id: 1, tags: ["1", "2"], name: "name1"}
    user2 = %User{id: 2, tags: ["a", "b", "c"], name: "name2"}

    {:ok, _} =
      TestRepo.batch_write(
        put: [
          {user1, condition: condition(:ignore)},
          {user2, condition: condition(:ignore)}
        ]
      )

    {:ok, _} =
      TestRepo.batch_write(
        delete: [
          {user1, condition: condition(:expect_exist)},
          {user2, condition: condition(:expect_exist)}
        ]
      )

    {:ok, [{User, users}]} =
      TestRepo.batch_get([
        [
          %User{id: 1},
          %User{id: 2}
        ]
      ])

    assert users == nil
  end

  test "repo batch write to delete with a default value" do
    user = %User2{id: "1", name: "username0", age: 30}
    user2 = %User2{id: "2", name: "username2", age: 25}

    {:ok, _} =
      TestRepo.batch_write(
        put: [
          {user, condition: condition(:ignore)},
          {user2, condition: condition(:ignore)}
        ]
      )

    {:ok, [{User2, results}]} =
      TestRepo.batch_write(
        delete: [
          {%User2{id: "1", age: 30}, condition: condition(:expect_exist, "name" == "username0")}
        ]
      )

    [{:ok, deleted_user2}] = results[:delete]
    assert deleted_user2.id == "1"

    {:ok, [{User2, results}]} =
      TestRepo.batch_write(
        delete: [
          {%User2{id: "2", age: 0},
           condition: condition(:expect_exist, "name" == "username2" and "age" == 25)}
        ]
      )

    [{:ok, deleted_user2}] = results[:delete]
    assert deleted_user2.id == "2"
  end

  test "optimistic_lock/3" do
    order =
      TestRepo.insert!(%Order{id: "1001", name: "name1"},
        condition: condition(:ignore),
        return_type: :pk
      )

    valid_change = Order.changeset(:update, order, %{name: "bar"})

    stale_change = Order.changeset(:update, order, %{name: "baz"})

    {:ok, order} = TestRepo.update(valid_change, condition: condition(:ignore))

    assert order.name == "bar" and order.lock_version == 2

    assert_raise Ecto.StaleEntryError, fn ->
      TestRepo.update(stale_change, condition: condition(:ignore))
    end

    # since we don't explicitly fetch the "lock_version" field in query,
    fetched_order =
      TestRepo.get(Order, [id: order.id, internal_id: order.internal_id], columns_to_get: ["name"])

    # the returned dataset of order struct will use the default value of "lock_version" as 1
    assert fetched_order.lock_version == 1

    # and this case will make the next update fail when we use `Ecto.Changeset.optimistic_lock/3` in the changeset
    # once we use the optimistic_lock, please remember append the fresh "version" field in the update operation for
    # the condition check.
    stale_change = Order.changeset(:update, fetched_order, %{name: "foo"})

    assert_raise Ecto.StaleEntryError, fn ->
      TestRepo.update(stale_change, condition: condition(:ignore))
    end

    # compare a success case to the previous stale error
    fetched_order =
      TestRepo.get(Order, [id: order.id, internal_id: order.internal_id],
        columns_to_get: ["name", "lock_version"]
      )

    assert fetched_order.lock_version == 2
    valid_change = Order.changeset(:update, fetched_order, %{name: "foo"})

    {:ok, order} =
      TestRepo.update(valid_change, condition: condition(:expect_exist, "name" == "bar"))

    assert order.name == "foo" and order.lock_version == 3
  end

  test "batch_write with embeds" do
    inserted_users =
      for index <- 1..3 do
        cars = [
          %User4.Car{name: "carname#{index}_a"},
          %User4.Car{name: "carname#{index}_b"},
        ]
        {:ok, inserted} = TestRepo.insert(%User4{id: "#{index}", cars: cars}, condition: condition(:ignore))
        inserted
      end

    [user1, user2, user3] = inserted_users

    changeset1 = Changeset.change(user1, info: %User4.Info{name: "bar"}, count: {:increment, 10})

    changeset2 = Changeset.change(%User4{id: "not_existed0"}, info: %User4.Info{name: "foo"})

    changeset3 =
      user3
      |> Ecto.Changeset.change(count: {:increment, 1})
      |> Ecto.Changeset.put_embed(:cars, [%User4.Car{name: "user3_new_car1"}, %User4.Car{name: "user3_new_car2"}])

    new_userid4 = %User4{id: "4", count: 1, info: %User4.Info{name: "bar4"}, cars: [%User4.Car{name: "car4name"}]}
    new_userid4_changeset =
      new_userid4
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.put_embed(:info, %{name: "bar4v2"})

    new_order_id = "orderbatchwrite_1"
    new_order_name = "orderbatchwrite_testname"

    writes = [
      update: [
        {changeset1, condition: condition(:expect_exist)},
        {changeset2, condition: condition(:expect_not_exist), return_type: :pk},
        {changeset3, condition: condition(:expect_exist), return_type: :pk}
      ],
      delete: [
        {user2, condition: condition(:expect_exist)}
      ],
      put: [
        {new_userid4_changeset, condition: condition(:expect_not_exist)},
        {%User4{id: "5", info: %User4.Info{name: "bar5"}, cars: [%User4.Car{name: "car5name"}]}, condition: condition(:expect_not_exist)},
        {%Order{id: new_order_id, name: new_order_name}, condition: condition(:ignore), return_type: :pk},
        {%User{id: 100, name: "username"}, condition: condition(:expect_not_exist)}
      ]
    ]

    {:ok, result} = TestRepo.batch_write(writes)

    result_to_user4 = Keyword.get(result, User4)

    # update in batch_write with `User4`
    [{:ok, update_user1}, {:ok, update_user2}, {:ok, update_user3}] = Keyword.get(result_to_user4, :update)

    assert update_user1.count == 10
    assert update_user1.cars == [%User4.Car{name: "carname1_a"}, %User4.Car{name: "carname1_b"}]
    assert update_user1.info == %User4.Info{name: "bar"}

    assert update_user2.id == "not_existed0" and update_user2.info.name == "foo"

    assert update_user3.id == "3" and update_user3.count == 1
    assert update_user3.cars == [%User4.Car{name: "user3_new_car1"}, %User4.Car{name: "user3_new_car2"}]

    # delete in batch_write with `User4`
    [{:ok, delete_user2}] = Keyword.get(result_to_user4, :delete)

    assert delete_user2.id == "2"
    assert delete_user2.cars == [%User4.Car{name: "carname2_a"}, %User4.Car{name: "carname2_b"}]

    assert TestRepo.one(user2) == nil

    # put in batch_write with `User4`
    [{:ok, put_user1}, {:ok, put_user2}] = Keyword.get(result_to_user4, :put)
    assert put_user1.id == "4"
    assert put_user1.cars == [%EctoTablestore.TestSchema.User4.Car{name: "car4name", status: nil}]
    assert put_user1.info == %User4.Info{name: "bar4v2"}

    assert put_user2.id == "5"
    assert put_user2.cars == [%EctoTablestore.TestSchema.User4.Car{name: "car5name", status: nil}]
    assert put_user2.info == %User4.Info{name: "bar5"}

    # put in batch_write with `User`
    result_to_user = Keyword.get(result, User)

    [{:ok, put_user}] = Keyword.get(result_to_user, :put)
    assert put_user.id == 100
    assert put_user.inserted_at == put_user.updated_at and is_integer(put_user.inserted_at) == true

    result_to_order = Keyword.get(result, Order)

    # put in batch_write with `Order`
    [{:ok, put_order}] = Keyword.get(result_to_order, :put)

    assert put_order.id == new_order_id and is_integer(put_order.internal_id) == true
    assert put_order.name == new_order_name
  end

end

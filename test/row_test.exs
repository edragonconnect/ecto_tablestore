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

  test "repo - embed schema c/r/u/d" do
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
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.put_embed(:info, info)
      |> Ecto.Changeset.put_embed(:item, embed_item)

    {status, _updated_result} =
      TestRepo.update(changeset, condition: condition(:expect_exist), return_type: :pk)

    assert :ok == status

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

    assert_raise Ecto.ConstraintError, fn ->
      TestRepo.insert(order, condition: condition(:expect_not_exist), return_type: :pk)
    end

    {status, saved_order} =
      TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

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

    {:ok, updated_order} = TestRepo.update(changeset, condition: condition(:expect_exist))

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
    user = %User{name: "username", level: 10, id: 1}
    {:ok, saved_user} = TestRepo.insert(user, condition: condition(:ignore))
    assert saved_user.updated_at == saved_user.inserted_at
    assert is_integer(saved_user.updated_at)

    user = TestRepo.get(User, id: 1)

    new_name = "username2"

    changeset = Ecto.Changeset.change(user, name: new_name, level: {:increment, 1})

    Process.sleep(1000)

    {:ok, updated_user} = TestRepo.update(changeset, condition: condition(:expect_exist))

    assert updated_user.level == 11
    assert updated_user.name == new_name
    assert updated_user.updated_at > updated_user.inserted_at

    TestRepo.delete(user, condition: condition(:expect_exist))
  end

  test "repo - insert/update with ecto types" do
    assert_raise Ecto.ConstraintError, ~r/OTSConditionCheckFail/, fn ->
      user = %User{
        name: "username2",
        profile: %{"level" => 1, "age" => 20},
        tags: ["tag_a", "tag_b"],
        id: 2
      }

      {:ok, _saved_user} = TestRepo.insert(user, condition: condition(:expect_exist))
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

  test "repo - get_range" do
    saved_orders =
      Enum.map(1..9, fn var ->
        order = %Order{id: "#{var}", desc: "desc#{var}", num: var, price: 20.5 * var}

        {:ok, saved_order} =
          TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

        saved_order
      end)

    start_pks = [{"id", "0a"}, {"internal_id", :inf_min}]
    end_pks = [{"id", "0b"}, {"internal_id", :inf_max}]

    {orders, next_start_primary_key} = TestRepo.get_range(Order, start_pks, end_pks)
    assert orders == nil and next_start_primary_key == nil

    start_pks = [{"id", "1"}, {"internal_id", :inf_min}, {"id", "1"}]
    end_pks = [{"id", "3"}, {"internal_id", :inf_max}, {"id", "3"}]
    {orders, next_start_primary_key} = TestRepo.get_range(Order, start_pks, end_pks)
    assert next_start_primary_key == nil
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

    for order <- saved_orders do
      TestRepo.delete(%Order{id: order.id, internal_id: order.internal_id},
        condition: condition(:expect_exist)
      )
    end
  end

  test "repo - stream_range" do
    saved_orders =
      Enum.map(1..9, fn var ->
        order = %Order{id: "#{var}", desc: "desc#{var}", num: var, price: 20.5 * var}

        {:ok, saved_order} =
          TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

        saved_order
      end)

    start_pks = [{"id", "0a"}, {"internal_id", :inf_min}]
    end_pks = [{"id", "0b"}, {"internal_id", :inf_max}]

    # since it is enumerable, no matched data will return `[]`.
    orders =
      Order
      |> TestRepo.stream_range(start_pks, end_pks, direction: :forward)
      |> Enum.to_list()

    assert orders == []

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

    backward_orders =
      Order
      |> TestRepo.stream_range(start_pks, end_pks, direction: :backward)
      |> Enum.to_list()

    assert orders == Enum.reverse(backward_orders)

    start_pks = [{"id", "1"}, {"internal_id", :inf_min}]
    end_pks = [{"id", "9"}, {"internal_id", :inf_max}]

    all_orders =
      Order
      |> TestRepo.stream_range(start_pks, end_pks, limit: 3)
      |> Enum.to_list()

    assert length(all_orders) == 9

    take_orders =
      Order
      |> TestRepo.stream_range(start_pks, end_pks, limit: 3)
      |> Enum.take(5)

    assert length(take_orders) == 5

    for order <- saved_orders do
      TestRepo.delete(%Order{id: order.id, internal_id: order.internal_id},
        condition: condition(:expect_exist)
      )
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
        saved_order2,
        {Order, [id: "order0", internal_id: saved_order0.internal_id],
         condition: condition(:ignore)},
        {user2, return_type: :pk}
      ],
      update: [
        {changeset_user1, return_type: :pk},
        {changeset_order1, return_type: :pk}
      ],
      put: [
        {order3, condition: condition(:ignore), return_type: :pk},
        {order4_changeset, condition: condition(:ignore), return_type: :pk},
        {user3, condition: condition(:expect_not_exist), return_type: :pk}
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

    {:ok, batch_write_put_order} = Keyword.get(order_batch_write_result, :put) |> List.first()
    assert batch_write_put_order.id == order3.id
    assert batch_write_put_order.desc == "desc3"
    assert batch_write_put_order.num == 10
    assert batch_write_put_order.price == 55.67

    {:ok, batch_write_put_order4} = Keyword.get(order_batch_write_result, :put) |> List.last()
    assert batch_write_put_order4.id == order4_changeset.data.id
    assert is_integer(batch_write_put_order4.internal_id) == true
    assert batch_write_put_order4.num == order4_changeset.changes.num

    user_batch_write_result = Keyword.get(result, User)

    {:ok, batch_write_update_user} = Keyword.get(user_batch_write_result, :update) |> List.first()
    assert batch_write_update_user.level == user1_lv + 1
    assert batch_write_update_user.name == "new_user_2"
    assert batch_write_update_user.id == 100

    {:ok, batch_write_delete_user} = Keyword.get(user_batch_write_result, :delete) |> List.first()
    assert batch_write_delete_user.level == 11
    assert batch_write_delete_user.id == 101
    assert batch_write_delete_user.name == "u2"

    {:ok, batch_write_put_user} = Keyword.get(user_batch_write_result, :put) |> List.first()
    assert batch_write_put_user.level == 12
    assert batch_write_put_user.id == 102
    assert batch_write_put_user.name == "u3"

    changeset_user3 = Changeset.change(batch_write_put_user, level: {:increment, 2})

    # failed case
    writes2 = [
      delete: [
        batch_write_put_user
      ],
      update: [
        changeset_user3
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
    {:ok, _} = TestRepo.delete(batch_write_put_order, condition: condition(:ignore))
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
        stale_error_message: stale_error_message
      )

    {^stale_error_message, error} = Keyword.get(invalid_changeset.errors, stale_error_field)
    assert error == [stale: true]

    {:ok, _} = TestRepo.delete(saved_order, condition: condition(:expect_exist))
  end

  test "repo - check_constraint" do
    check_constraint_field = :condition
    check_constraint_name = "OTSConditionCheckFail"
    check_constraint_message = "ots condition check fail"

    user =
      %User2{id: "100"}
      |> Ecto.Changeset.change(name: "name2")
      |> Ecto.Changeset.check_constraint(check_constraint_field,
        name: check_constraint_name,
        message: check_constraint_message
      )

    {:error, invalid_changeset} =
      TestRepo.insert(user, condition: condition(:expect_exist), return_type: :pk)

    {^check_constraint_message, error_constraint} =
      Keyword.get(invalid_changeset.errors, check_constraint_field)

    error_constraint_name = Keyword.get(error_constraint, :constraint_name)
    # Use ots's error code as check_constraint_name.
    assert error_constraint_name == check_constraint_name
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
          user1,
          user2
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
end

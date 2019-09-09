defmodule EctoTablestoreTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.{Order, User}
  alias EctoTablestore.TestRepo
  alias Ecto.Changeset

  import EctoTablestore.Query, only: [condition: 2, condition: 1, filter: 1]

  setup_all do
    TestHelper.setup_all()
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
      {insert_result, message} =
        TestRepo.insert(order, condition: condition(:expect_not_exist), return_type: :pk)

      assert insert_result == :error

      assert String.contains?(
               message,
               "Invalid expect:EXPECT_NOT_EXIST when modify row with pk auto increment"
             ) == true
    end

    {status, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)
    assert status == :ok
    saved_order_internal_id = saved_order.internal_id

    assert saved_order.id == input_id
    # autogenerate primary key return in schema
    assert saved_order_internal_id != nil and is_integer(saved_order_internal_id) == true

    order_with_non_exist_num = %Order{id: input_id, internal_id: saved_order_internal_id, num: 2}
    query_result = TestRepo.one(order_with_non_exist_num)
    assert query_result == nil

    # `num` attribute field will be used in condition filter
    order_with_matched_num = %Order{
      id: input_id,
      internal_id: saved_order_internal_id,
      num: input_num
    }

    query_result_by_one = TestRepo.one(order_with_matched_num)

    assert query_result_by_one != nil
    assert query_result_by_one.desc == input_desc
    assert query_result_by_one.num == input_num
    assert query_result_by_one.name == input_order_name
    assert query_result_by_one.success? == input_success
    assert query_result_by_one.price == input_price

    # query and return fields in `columns_to_get`
    query_result2_by_one = TestRepo.one(order_with_matched_num, columns_to_get: ["num", "desc"])
    assert query_result2_by_one.desc == input_desc
    assert query_result2_by_one.num == input_num
    assert query_result2_by_one.name == nil
    assert query_result2_by_one.success? == nil
    assert query_result2_by_one.price == nil

    # Optional use case `get/3`
    query_result_by_get = TestRepo.get(Order, id: input_id, internal_id: saved_order_internal_id)
    assert query_result_by_get == query_result_by_one

    query_result2_by_get =
      TestRepo.get(Order, [id: input_id, internal_id: saved_order_internal_id],
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

    order = %Order{id: input_id, internal_id: saved_order_internal_id}

    assert_raise Ecto.StaleEntryError, fn ->
      TestRepo.delete(order, condition: condition(:expect_exist, "num" == "invalid_num"))
    end

    {result, _} = TestRepo.delete(order, condition: condition(:expect_exist, "num" == input_num))
    assert result == :ok
  end

  test "repo - update" do
    input_id = "1001"
    input_desc = "order_desc"
    input_num = 100
    increment = 1
    input_price = 99.9
    order = %Order{id: input_id, desc: input_desc, num: input_num, price: input_price}
    {:ok, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

    assert saved_order.price == input_price

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
    assert updated_order.num == input_num + increment
    assert updated_order.name == updated_order_name

    order = TestRepo.get(Order, id: input_id, internal_id: saved_order.internal_id)

    assert order.desc == nil
    assert order.num == input_num + increment
    assert order.name == updated_order_name

    TestRepo.delete(%Order{id: input_id, internal_id: saved_order.internal_id},
      condition: condition(:expect_exist)
    )
  end

  test "repo - get_range" do
    saved_orders =
      Enum.map(1..9, fn var ->
        order = %Order{id: "#{var}", desc: "desc#{var}", num: var, price: 20.5 * var}
        {:ok, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)
        saved_order
      end)

    start_pks = [{"id", "1"}, {"internal_id", :inf_min}]
    end_pks = [{"id", "3"}, {"internal_id", :inf_max}]
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

  test "repo - batch_get" do
    {saved_orders, saved_users} =
      Enum.reduce(1..3, {[], []}, fn var, {cur_orders, cur_users} ->
        order = %Order{id: "#{var}", desc: "desc#{var}", num: var, price: 1.8 * var}
        {:ok, saved_order} = TestRepo.insert(order, condition: condition(:ignore), return_type: :pk)

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

    query_orders = Keyword.get(result, Order)
    assert length(query_orders) == 1

    query_users = Keyword.get(result, User)
    assert length(query_users) == 2

    for query_user <- query_users do
      assert query_user.level != nil
    end

    # provide attribute column `name` in schema, will implicitly use it in the filter and add `name` into columns_to_get if specially set columns_to_get.
    requests2 = [
      {[%User{id: 1, name: "name1"}, %User{id: 2, name: "name2"}], columns_to_get: ["level"]}
    ]

    {:ok, result2} = TestRepo.batch_get(requests2)

    IO.puts(">>> result2: #{inspect(result2)}")
    query_users2 = Keyword.get(result2, User)

    assert length(query_users2) == 2

    for query_user <- query_users2 do
      assert query_user.name != nil
      assert query_user.level != nil
    end

    assert_raise RuntimeError, fn ->
      # one schema provide `name` attribute, another schema only has primary key,
      # in this case, there exist conflict will raise a RuntimeError for 
      requests_invalid = [
        [%User{id: 1, name: "name1"}, %User{id: 2}]
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

    # After update User(id: 1)'s name as `new_name1`, in the next batch get, attribute columns will be used in filter.
    # 
    # The following case will only return User(id: 2) in batch get.
    #
    changeset = Ecto.Changeset.change(%User{id: 1}, name: "new_name1")
    TestRepo.update(changeset, condition: condition(:expect_exist))

    requests3_1 = [
      [%User{id: 1, name: "name1"}, %User{id: 2, name: "name2"}]
    ]

    {:ok, result3_1} = TestRepo.batch_get(requests3_1)

    query_result3_1 = Keyword.get(result3_1, User)

    assert length(query_result3_1) == 1
    query3_1_user = List.first(query_result3_1)
    assert query3_1_user.id == 2
    assert query3_1_user.name == "name2"

    requests4 = [
      [%User{id: 1, name: "name1"}]
    ]

    {:ok, result4} = TestRepo.batch_get(requests4)
    assert Keyword.get(result4, User) == nil

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
        {Order, [id: "order0", internal_id: saved_order0.internal_id], condition: condition(:ignore)},
        {user2, return_type: :pk}
      ],
      update: [
        {changeset_user1, return_type: :pk},
        {changeset_order1, return_type: :pk}
      ],
      put: [
        {order3, condition: condition(:ignore), return_type: :pk},
        {user3, condition: condition(:expect_not_exist), return_type: :pk}
      ]
    ]

    {:ok, result} = TestRepo.batch_write(writes)
    IO.puts("** batch_write result: #{inspect(result)} **")

    order_batch_write_result = Keyword.get(result, Order)

    {:ok, batch_write_update_order} = Keyword.get(order_batch_write_result, :update) |> List.first()
    assert batch_write_update_order.num == order1_num + 1
    assert batch_write_update_order.id == order1.id
    assert batch_write_update_order.internal_id == saved_order1.internal_id
    assert batch_write_update_order.price == nil

    [{:ok, batch_write_delete_order2}, {:ok, batch_write_delete_order0}] = Keyword.get(order_batch_write_result, :delete)

    assert batch_write_delete_order2.id == saved_order2.id
    assert batch_write_delete_order2.internal_id == saved_order2.internal_id

    assert batch_write_delete_order0.id == saved_order0.id
    assert batch_write_delete_order0.internal_id == saved_order0.internal_id

    {:ok, batch_write_put_order} = Keyword.get(order_batch_write_result, :put) |> List.first()
    assert batch_write_put_order.id == order3.id
    assert batch_write_put_order.desc == "desc3"
    assert batch_write_put_order.num == 10
    assert batch_write_put_order.price == 55.67

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

    changeset_user3 =
      Changeset.change(batch_write_put_user, level: {:increment, 2})

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

    {:error, batch_write_update_response} = Keyword.get(fail_batch_write_result, :update) |> List.first()
    assert batch_write_update_response.is_ok == false
    {:error, batch_write_delete_response} = Keyword.get(fail_batch_write_result, :delete) |> List.first()
    assert batch_write_delete_response.is_ok == false

    {:ok, _} = TestRepo.delete(batch_write_put_user, condition: condition(:expect_exist))
    {:ok, _} = TestRepo.delete(batch_write_update_user, condition: condition(:expect_exist))
    {:ok, _} = TestRepo.delete(batch_write_put_order, condition: condition(:ignore))
    {:ok, _} = TestRepo.delete(batch_write_update_order, condition: condition(:ignore))
  end
end

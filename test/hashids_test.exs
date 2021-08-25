defmodule EctoTablestore.HashidsTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.{Post, Post2}
  alias EctoTablestore.TestRepo

  @instance EDCEXTestInstance

  import EctoTablestore.Query, only: [condition: 1]

  setup_all do
    TestHelper.setup_all()

    post_table = Post.__schema__(:source)

    pk = "keyid"

    ExAliyunOts.create_table(
      @instance,
      post_table,
      [{pk, :string}]
    )

    post2_table = Post2.__schema__(:source)

    pk = "id"

    ExAliyunOts.create_table(
      @instance,
      post2_table,
      [{pk, :string}]
    )

    # There will use `ecto_tablestore_default_seq` as global
    EctoTablestore.Sequence.create(@instance)

    on_exit(fn ->

      Enum.map([post_table, post2_table], fn table ->
        ExAliyunOts.delete_table(@instance, table)

        key = Ecto.Adapters.Tablestore.key_to_global_sequence(table, pk)

        ExAliyunOts.delete_row(
          @instance,
          "ecto_tablestore_default_seq",
          [{"name", key}],
          condition: condition(:expect_exist)
        )
      end)

    end)
  end

  test "generate hashids as primary key when insert" do
    post = %Post{content: "test"}

    {:ok, post2} =
      TestRepo.insert(post, condition: condition(:expect_not_exist), return_type: :pk)

    new_content = "update test content"
    changeset = Ecto.Changeset.change(post2, content: new_content)

    {:ok, update_post} = TestRepo.update(changeset, condition: condition(:expect_exist))

    assert update_post.content == new_content

    fetch_post = TestRepo.get(Post, keyid: post2.keyid)
    assert fetch_post.content == new_content

    fetch_post2 = TestRepo.one(update_post)
    assert fetch_post2.content == new_content

    new_post = %Post{content: "new content"}

    {:ok, new_post2} =
      TestRepo.insert(new_post, condition: condition(:expect_not_exist), return_type: :pk)

    assert new_post2.keyid != post2.keyid
    {:keyid, :keyid, {:parameterized, Ecto.Hashids, opts}} = Post.__schema__(:autogenerate_id)

    # no Hashids related options defined in the `Post` schema
    assert opts == [schema: Post]

    app = Application.get_application(EctoTablestore.Application)
    [{Post, config_hashids_opts}] = Application.get_env(app, :hashids)
    hashids = Hashids.new(config_hashids_opts)

    [id2] = Hashids.decode!(hashids, new_post2.keyid)
    [id] = Hashids.decode!(hashids, post2.keyid)
    assert id2 == id + 1
  end

  test "generate hashids when batch write" do
    p0 = %Post{content: "p0"}
    {:ok, saved_p0} = TestRepo.insert(p0, condition: condition(:ignore), return_type: :pk)
    p1 = %Post{content: "p1"}
    {:ok, saved_p1} = TestRepo.insert(p1, condition: condition(:ignore), return_type: :pk)
    p2 = %Post{content: "p2"}
    {:ok, saved_p2} = TestRepo.insert(p2, condition: condition(:ignore), return_type: :pk)
    p3 = %Post{content: "p3"}

    new_p2_content = "new p2 content"

    changeset_post2 =
      Post
      |> TestRepo.get(keyid: saved_p2.keyid)
      |> Ecto.Changeset.change(content: new_p2_content)

    writes = [
      delete: [
        {saved_p0, entity_full_match: true},
        {Post, [keyid: saved_p1.keyid], condition: condition(:ignore)}
      ],
      update: [
        {changeset_post2, entity_full_match: true, return_type: :pk}
      ],
      put: [
        {p3, condition: condition(:expect_not_exist), return_type: :pk},
        {%Post2{content: "post2_p0"}, condition: condition(:expect_not_exist), return_type: :pk}
      ]
    ]

    {:ok, result} = TestRepo.batch_write(writes)

    post_batch_write_result = Keyword.get(result, Post)

    [{:ok, put_item}] = post_batch_write_result[:put]
    assert put_item.content == "p3" and is_bitstring(put_item.keyid) == true

    post2_batch_write_result = Keyword.get(result, Post2)
    [{:ok, put_item2}] = post2_batch_write_result[:put]
    assert put_item2.content == "post2_p0" and is_bitstring(put_item2.id) == true

    [{:ok, delete_item1}, {:ok, delete_item2}] = post_batch_write_result[:delete]
    assert delete_item1.keyid == saved_p0.keyid
    assert delete_item2.keyid == saved_p1.keyid
    [{:ok, update_item}] = post_batch_write_result[:update]
    assert update_item.keyid == saved_p2.keyid and update_item.content == new_p2_content
  end

  describe "define :hashids options in app env" do
    test "make sure hashids configurable in runtime" do
      app = Application.get_application(EctoTablestore.Application)

      [{Post, config_hashids_opts}] = Application.get_env(app, :hashids)

      {:keyid, :keyid, {:parameterized, Ecto.Hashids, opts} = type} = Post.__schema__(:autogenerate_id)
      # no Hashids related options defined when schema initialized
      assert opts == [schema: Post]

      assert Ecto.Type.dump(type, 1) == {:ok, Hashids.new(config_hashids_opts) |> Hashids.encode(1)}

      new_salt = "1234"
      new_min_len = 12
      Application.put_env(app, :hashids, [{Post, salt: new_salt, min_len: new_min_len}])

      {:keyid, :keyid, {:parameterized, Ecto.Hashids, opts}} = Post.__schema__(:autogenerate_id)
      assert opts == [schema: Post]

      assert Ecto.Type.dump(type, 100) == {:ok, Hashids.new([salt: new_salt, min_len: new_min_len]) |> Hashids.encode(100)}

      assert_raise ArgumentError, ~r/expect a keyword as options to Hashids/, fn ->
        Application.put_env(app, :hashids, [{Post, :invalid_hashids_config}])
        Ecto.Type.dump(type, 1)
      end
    end
  end

  describe "define :hashids options in field" do
    test "verify dump with definition" do
      alphabet = "1234567890cfhistu"
      min_len = 5
      salt = "testsalt"

      {:id, :id, {:parameterized, Ecto.Hashids, opts} = type} = Post2.__schema__(:autogenerate_id)
      assert opts[:alphabet] == alphabet and opts[:min_len] == min_len and opts[:salt] == salt

      assert Ecto.Type.dump(type, 100) == {:ok, Hashids.new([salt: salt, min_len: min_len, alphabet: alphabet]) |> Hashids.encode(100)}
    end

    test "always use field :hashids options once defined" do
      # before change Post2's :hashids options in env
      {:id, :id, {:parameterized, Ecto.Hashids, _opts} = type} = Post2.__schema__(:autogenerate_id)
      {:ok, value} = Ecto.Type.dump(type, 100)
      assert is_integer(String.to_integer(value)) == true and String.length(value) == 5

      app = Application.get_application(EctoTablestore.Application)
      alphabet = "1234567890cfhistu"
      new_salt = "new_salt"
      new_min_len = 20
      Application.put_env(app, :hashids, [{Post2, salt: new_salt, min_len: new_min_len, alphabet: alphabet}])

      # after change Post2's :hashids options in env
      {:ok, value2} = Ecto.Type.dump(type, 100)
      assert value == value2

      assert value2 != Hashids.new([salt: new_salt, min_len: new_min_len, alphabet: alphabet]) |> Hashids.encode(100)
    end
  end
end

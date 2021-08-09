defmodule EctoTablestore.HashidsTest do
  use ExUnit.Case

  alias EctoTablestore.TestSchema.{Post, Post2}
  alias EctoTablestore.TestRepo

  @instance EDCEXTestInstance

  import EctoTablestore.Query, only: [condition: 1]

  setup_all do
    TestHelper.setup_all()

    table = Post.__schema__(:source)

    pk = "keyid"

    ExAliyunOts.create_table(
      @instance,
      table,
      [{pk, :string}]
    )

    # There will use `ecto_tablestore_default_seq` as global
    EctoTablestore.Sequence.create(@instance)

    on_exit(fn ->
      ExAliyunOts.delete_table(@instance, table)

      key = Ecto.Adapters.Tablestore.key_to_global_sequence(table, pk)

      ExAliyunOts.delete_row(
        @instance,
        "ecto_tablestore_default_seq",
        [{"name", key}],
        condition: condition(:expect_exist)
      )
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
    hashids = Post.hashids(:keyid)
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
        {p3, condition: condition(:expect_not_exist), return_type: :pk}
      ]
    ]

    {:ok, result} = TestRepo.batch_write(writes)

    post_batch_write_result = Keyword.get(result, Post)

    [{:ok, put_item}] = post_batch_write_result[:put]
    assert put_item.content == "p3"
    [{:ok, delete_item1}, {:ok, delete_item2}] = post_batch_write_result[:delete]
    assert delete_item1.keyid == saved_p0.keyid
    assert delete_item2.keyid == saved_p1.keyid
    [{:ok, update_item}] = post_batch_write_result[:update]
    assert update_item.keyid == saved_p2.keyid and update_item.content == new_p2_content
  end

  test "schema generate hashids function" do
    hashids1 = Post.hashids(:keyid)
    value = Hashids.encode(hashids1, 1)
    assert String.length(value) == 2
    [num] = Hashids.decode!(hashids1, value)
    assert num == 1

    hashids2 = Post2.hashids(:id)
    value2 = Hashids.encode(hashids2, 10)
    assert String.length(value2) == 5
    assert is_integer(String.to_integer(value2)) == true

    [num2] = Hashids.decode!(hashids2, value2)
    assert num2 == 10
  end

  test "make sure hashids configurable in runtime" do
    app = Application.get_application(EctoTablestore.Application)

    [{Post, config_hashids_opts}] = Application.get_env(app, :hashids)

    hashids = Post.hashids(:keyid)
    assert "#{hashids.salt}" == config_hashids_opts[:salt] and hashids.min_len == config_hashids_opts[:min_len]

    new_salt = "1234"
    new_min_len = 12
    Application.put_env(app, :hashids, [{Post, salt: new_salt, min_len: new_min_len}])

    hashids = Post.hashids(:keyid)
    assert "#{hashids.salt}" == new_salt and hashids.min_len == new_min_len

    assert_raise RuntimeError, ~r/:invalid_hashids_config/, fn ->
      Application.put_env(app, :hashids, [{Post, :invalid_hashids_config}])
      Post.hashids(:keyid)
    end
  end
end

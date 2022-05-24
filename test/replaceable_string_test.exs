defmodule EctoTablestore.ReplaceableStringTest do
  use ExUnit.Case

  alias Ecto.Changeset
  alias EctoTablestore.TestRepo
  alias __MODULE__

  import EctoTablestore.Query, only: [condition: 1]

  defmodule ReplaceableSchema do
    use EctoTablestore.Schema

    schema "test_replaceable_schema" do
      field(:id, :string, primary_key: true)

      embeds_one(:profile, ReplaceableStringTest.Profile, on_replace: :delete)

      field(:field_on_dump_replace, Ecto.ReplaceableString,
        on_dump: [pattern: "test", replacement: "TEST", options: [global: false]])
      field(:field_on_dump_array_replace, {:array, Ecto.ReplaceableString},
        on_dump: [pattern: ",", replacement: "-"])

      field(:field_on_load_replace, Ecto.ReplaceableString,
        on_load: [pattern: ~r/foo/, replacement: "FOO"])
      field(:field_on_load_array_replace, {:array, Ecto.ReplaceableString},
        on_load: [pattern: ~r(http://), replacement: "https://"])

      field(:price, :decimal)
    end
  end

  @table ReplaceableSchema.__schema__(:source)

  defmodule Profile do
    use Ecto.Schema

    @primary_key false
    embedded_schema do
      field(:name, :string)
      field(:urls, {:array, Ecto.ReplaceableString},
        on_load: [pattern: ~r(http://), replacement: "https://"])
      field(:comment, Ecto.ReplaceableString,
        on_dump: [pattern: "", replacement: "."])
    end

    def changeset(schema, params) do
      Ecto.Changeset.cast(schema, params, [:name, :urls, :comment])
    end
  end

  setup_all do
    TestHelper.setup_all()

    create_table()

    Process.sleep(1_000)
  end

  defp cast(schema, params, embed, opts \\ []) do
    schema
    |> Changeset.cast(params, ~w())
    |> Changeset.cast_embed(embed, opts)
  end

  test "bad values" do
    message = "Ecto.ReplaceableString type must both have a `:pattern` option specified as a string or a regex\n" <>
      "type and a `:replacement` option as a string type"

    assert_raise ArgumentError, ~r/#{message}/, fn ->
      defmodule SchemaInvalidReplaceable do
        use EctoTablestore.Schema

        schema "invalid" do
          field :id, :string, primary_key: true
          field :name, Ecto.ReplaceableString,
            on_dump: [pattern: ~r/pattern/]
        end
      end
    end

    assert_raise ArgumentError, ~r/#{message}/, fn ->
      defmodule SchemaInvalidReplaceable do
        use EctoTablestore.Schema

        schema "invalid" do
          field :id, :string, primary_key: true
          field :name, Ecto.ReplaceableString,
            on_dump: [replacement: "test"]
        end
      end
    end
  end

  describe "no any options to Ecto.ReplaceableString" do

    defmodule ReplaceableAsString do
      # in this case, the defined type of field is :string
      use EctoTablestore.Schema

      schema "replaceable_as_string" do
        field :id, :string, primary_key: true
        field :name, Ecto.ReplaceableString
        field :tags, {:array, Ecto.ReplaceableString}
      end
    end

    test "type" do
      assert Ecto.Type.type(ReplaceableAsString.__schema__(:type, :name)) == :string
      assert Ecto.Type.type(ReplaceableAsString.__schema__(:type, :tags)) == {:array, :string}
    end

    test "casts nil" do
      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableAsString{}, %{name: nil}, [:name])
      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableAsString{}, %{tags: nil}, [:tags])
    end

    test "casts strings" do
      assert %Changeset{valid?: true, changes: changes} = Changeset.cast(%ReplaceableAsString{}, %{tags: ["t1", "t2", "t3"]}, [:tags])
      assert changes == %{tags: ["t1", "t2", "t3"]}
    end

    test "cast string" do
      assert %Changeset{valid?: true, changes: changes} = Changeset.cast(%ReplaceableAsString{}, %{name: "helloname"}, [:name])
      assert changes == %{name: "helloname"}
    end
  end

  test "type" do
    assert Ecto.Type.type(ReplaceableSchema.__schema__(:type, :field_on_dump_array_replace)) == {:array, :string}
    assert Ecto.Type.type(ReplaceableSchema.__schema__(:type, :field_on_dump_replace)) == :string

    assert Ecto.Type.type(ReplaceableSchema.__schema__(:type, :profile)) == {:map, :any}
    assert Ecto.Type.type(Profile.__schema__(:type, :urls)) == {:array, :string}
    assert Ecto.Type.type(Profile.__schema__(:type, :comment)) == :string
  end

  describe "cast" do
    test "casts nil" do
      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableSchema{}, %{field_on_dump_replace: nil}, [:field_on_dump_replace])

      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableSchema{}, %{field_on_dump_array_replace: nil}, [:field_on_dump_array_replace])

      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableSchema{}, %{field_on_load_replace: nil}, [:field_on_load_replace])

      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableSchema{}, %{field_on_load_array_replace: nil}, [:field_on_load_array_replace])

      assert %Changeset{valid?: true} = Changeset.cast(%ReplaceableSchema{profile: %Profile{urls: nil}}, %{}, ~w())
    end

    test "casts strings" do
      assert %Changeset{valid?: true, changes: changes} = Changeset.cast(%ReplaceableSchema{}, %{field_on_dump_array_replace: ["a,a", "b.b", "c#c"]}, [:field_on_dump_array_replace])
      assert changes == %{field_on_dump_array_replace: ["a,a", "b.b", "c#c"]}

      assert %Changeset{valid?: true, changes: changes} = Changeset.cast(%ReplaceableSchema{}, %{field_on_load_array_replace: ["foo", "bar", "baz"]}, [:field_on_load_array_replace])
      assert changes == %{field_on_load_array_replace: ["foo", "bar", "baz"]}

      assert %Changeset{valid?: true, changes: %{profile: profile_changes}} = cast(%ReplaceableSchema{}, %{profile: %{urls: ["a", "b", "c"]}}, :profile)
      assert profile_changes.changes == %{urls: ["a", "b", "c"]}
      assert profile_changes.valid? == true
    end

    test "casts string" do
      assert %Changeset{valid?: true, changes: changes} = Changeset.cast(%ReplaceableSchema{}, %{field_on_dump_replace: "test test test"}, [:field_on_dump_replace])
      assert changes == %{field_on_dump_replace: "test test test"}

      assert %Changeset{valid?: true, changes: changes} = Changeset.cast(%ReplaceableSchema{}, %{field_on_load_replace: "http://example.bar.foo"}, [:field_on_load_replace])
      assert changes == %{field_on_load_replace: "http://example.bar.foo"}

      assert %Changeset{valid?: true, changes: %{profile: profile_changes}} = cast(%ReplaceableSchema{}, %{profile: %{comment: "test comment"}}, :profile)
      assert profile_changes.changes == %{comment: "test comment"}
      assert profile_changes.valid? == true
    end

    test "reject bad strings" do
      type = ReplaceableSchema.__schema__(:type, :field_on_dump_array_replace)
      assert %Changeset{
               valid?: false,
               changes: %{},
               errors: [field_on_dump_array_replace: {"is invalid", [type: ^type, validation: :cast]}]
             } = Changeset.cast(%ReplaceableSchema{}, %{field_on_dump_array_replace: ["a,a", "b.b", 1]}, [:field_on_dump_array_replace])

      type = ReplaceableSchema.__schema__(:type, :field_on_dump_replace)
      assert %Changeset{
               valid?: false,
               changes: %{},
               errors: [field_on_dump_replace: {"is invalid", [type: ^type, validation: :cast]}]
             } = Changeset.cast(%ReplaceableSchema{}, %{field_on_dump_replace: %{"a" => "b"}}, [:field_on_dump_replace])

      type = ReplaceableSchema.__schema__(:type, :field_on_load_array_replace)
      assert %Changeset{
               valid?: false,
               changes: %{},
               errors: [field_on_load_array_replace: {"is invalid", [type: ^type, validation: :cast]}]
             } = Changeset.cast(%ReplaceableSchema{}, %{field_on_load_array_replace: [true, 1.0, "a,a"]}, [:field_on_load_array_replace])

      type = ReplaceableSchema.__schema__(:type, :field_on_load_replace)
      assert %Changeset{
               valid?: false,
               changes: %{},
               errors: [field_on_load_replace: {"is invalid", [type: ^type, validation: :cast]}]
             } = Changeset.cast(%ReplaceableSchema{}, %{field_on_load_replace: true}, [:field_on_load_replace])

      type = Profile.__schema__(:type, :urls)
      assert %Changeset{
               valid?: false,
               changes: %{
                 profile: %Changeset{
                   valid?: false,
                   errors: [urls: {"is invalid", [type: ^type, validation: :cast]}]
                 }
               },
               errors: []
             } = cast(%ReplaceableSchema{}, %{profile: %{urls: [1, "a"]}}, :profile)
    end
  end

  describe "dump and load" do
    test "dump with valid values" do
      assert %ReplaceableSchema{field_on_dump_array_replace: ["a,a", "b", "c"], id: id} =
        TestRepo.insert!(%ReplaceableSchema{id: "1", field_on_dump_array_replace: ["a,a", "b", "c"]}, condition: condition(:ignore))

      # verify to read from server
      assert %ReplaceableSchema{field_on_dump_array_replace: ["a-a", "b", "c"]} = TestRepo.one(%ReplaceableSchema{id: id})

      assert %ReplaceableSchema{field_on_dump_replace: "test hello test", id: id} =
        TestRepo.insert!(%ReplaceableSchema{id: "2", field_on_dump_replace: "test hello test"}, condition: condition(:ignore))

      # verify to read from server
      assert %ReplaceableSchema{field_on_dump_replace: "TEST hello test"} = TestRepo.one(%ReplaceableSchema{id: id})
    end

    test "load with valid values" do
      init = ["http://a", "http://b", "foo"]

      assert %ReplaceableSchema{field_on_load_array_replace: ^init, id: id} =
        TestRepo.insert!(%ReplaceableSchema{id: "3", field_on_load_array_replace: init}, condition: condition(:ignore))

      # verify to read from server, but replacement happend on load
      assert %ReplaceableSchema{field_on_load_array_replace: ["https://a", "https://b", "foo"]} = TestRepo.one(%ReplaceableSchema{id: id})

      # and actually, the persistence data is same as the original insert data (init)
      {:ok, response} = ExAliyunOts.get_row(instance(), @table, [{"id", id}])
      {_, attrs} = response.row
      expected_value = Jason.encode!(init)
      assert [{"field_on_load_array_replace", ^expected_value, _}] = attrs

      value = "foo bar foo baz"
      assert %ReplaceableSchema{field_on_load_replace: ^value, id: id} =
        TestRepo.insert!(%ReplaceableSchema{id: "4", field_on_load_replace: value}, condition: condition(:ignore))

      # verify to read from server, but replacement happend on load
      assert %ReplaceableSchema{field_on_load_replace: "FOO bar FOO baz"} = TestRepo.one(%ReplaceableSchema{id: id})

      # and actually, the persistence data is same as the original insert data (init)
      {:ok, response} = ExAliyunOts.get_row(instance(), @table, [{"id", id}])
      {_, attrs} = response.row
      assert [{"field_on_load_replace", ^value, _}] = attrs
    end

    test "load with embeds" do
      init = ["http://A1", "http://B1"]

      assert %ReplaceableSchema{profile: %Profile{urls: ^init}, id: id} =
        TestRepo.insert!(%ReplaceableSchema{id: "5", profile: %{urls: ["http://A1", "http://B1"]}}, condition: condition(:ignore))

      assert %ReplaceableSchema{profile: %Profile{urls: ["https://A1", "https://B1"]}} =
        TestRepo.one(%ReplaceableSchema{id: id})

      # and actually, the persistence data is same as the original insert data (init)
      {:ok, response} = ExAliyunOts.get_row(instance(), @table, [{"id", id}])
      {_, attrs} = response.row
      [{"profile", value, _}] = attrs
      assert Jason.decode!(value)["urls"] == init
    end

    test "dump with embeds" do
      assert %ReplaceableSchema{profile: %Profile{comment: "test"}, id: id} =
        TestRepo.insert!(%ReplaceableSchema{id: "6", profile: %{comment: "test"}}, condition: condition(:ignore))

      expected_saved_value = ".t.e.s.t."

      # verify to read from server
      assert %ReplaceableSchema{profile: %Profile{comment: ^expected_saved_value}} =
        TestRepo.one(%ReplaceableSchema{id: id})

      # double check
      {:ok, response} = ExAliyunOts.get_row(instance(), @table, [{"id", id}])
      {_, attrs} = response.row
      [{"profile", value, _}] = attrs
      assert Jason.decode!(value)["comment"] == expected_saved_value
    end
  end

  test "decimal field type" do
    assert %ReplaceableSchema{price: 1.09, id: id} =
      TestRepo.insert!(%ReplaceableSchema{id: "7", price: 1.09}, condition: condition(:ignore))

    assert %ReplaceableSchema{price: %Decimal{} = price, id: ^id} =
      TestRepo.insert!(%ReplaceableSchema{id: id, price: Decimal.new("11.099")}, condition: condition(:ignore))

    assert Decimal.to_string(price) == "11.099"

    assert %ReplaceableSchema{price: %Decimal{} = price, id: ^id} =
      TestRepo.one(%ReplaceableSchema{id: id})

    assert Decimal.to_string(price) == "11.099"
  end

  defp create_table() do
    ExAliyunOts.create_table(instance(), @table, [{"id", :string}])
  end

  defp instance() do
    %{instance: instance} = Ecto.Adapter.lookup_meta(TestRepo)
    instance
  end

end

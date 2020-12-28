defmodule EctoTablestore.TestSchema.Order do
  use EctoTablestore.Schema
  import Ecto.Changeset

  tablestore_schema "ecto_ots_test_order" do
    field(:id, :binary_id, primary_key: true, autogenerate: false)
    field(:internal_id, :id, primary_key: true, autogenerate: true)
    field(:name, :string)
    field(:desc)
    field(:num, :integer)
    field(:success?, :boolean)
    field(:price, :float)
  end

  def test_changeset(order, params \\ %{}) do
    order
    |> cast(params, [:id, :name, :num])
    |> validate_required([:id, :name, :num])
  end
end

defmodule EctoTablestore.TestSchema.User do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_user" do
    field(:id, :id, primary_key: true)
    field(:name, :string)
    field(:level, :integer)

    field(:naive_dt, :naive_datetime)
    field(:dt, :utc_datetime)

    field(:profile, :map)
    field(:tags, {:array, :string})

    timestamps()
  end
end

defmodule EctoTablestore.TestSchema.Student do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_student" do
    field(:partition_key, :binary_id, primary_key: true)
    field(:class, :string)
    field(:name, :string)
    field(:age, :integer)
    field(:score, :float)
    field(:is_actived, :boolean)
    field(:comment, :string)
    field(:content, :string)
  end
end

defmodule EctoTablestore.TestSchema.Page do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_page" do
    field(:pid, :id, primary_key: true, autogenerate: true)
    field(:name, :string, primary_key: true)
    field(:content, :string)
    field(:age, :integer)
  end
end

defmodule EctoTablestore.TestSchema.User2 do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_user2" do
    field(:id, :string, primary_key: true)
    field(:age, :integer)
    field(:name, :string, default: "user_name_123")

    timestamps(
      type: :naive_datetime,
      autogenerate: {Ecto.Schema, :__timestamps__, [:naive_datetime]}
    )
  end
end

defmodule EctoTablestore.TestSchema.User3 do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_user3" do
    field(:id, :string, primary_key: true)
    field(:name, :string)
  end
end

defmodule EctoTablestore.TestSchema.User4 do
  use EctoTablestore.Schema

  tablestore_schema "test_embed_user4" do
    field(:id, :string, primary_key: true)

    embeds_many :cars, Car, primary_key: false do
      field(:name, :string)
      field(:status, Ecto.Enum, values: [:foo, :bar, :baz])
    end

    embeds_one :info, Info, primary_key: false, on_replace: :update do
      field(:name, :string)
      field(:money, :decimal)
      field(:status, Ecto.Enum, values: [:foo, :bar, :baz])
    end

    embeds_one(:item, EctoTablestore.TestSchema.EmbedItem, on_replace: :update)
  end
end

defmodule EctoTablestore.TestSchema.EmbedItem do
  use Ecto.Schema
  @primary_key false
  embedded_schema do
    field(:name, :string)
  end
end

defmodule EctoTablestore.TestSchema.Post do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_post" do
    field(:keyid, :hashids, primary_key: true, autogenerate: true)
    field(:content, :string)
  end
end

defmodule EctoTablestore.TestSchema.Post2 do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_post2" do
    field(:id, EctoTablestore.Hashids,
      primary_key: true,
      autogenerate: true,
      hashids: [alphabet: "1234567890cfhistu", min_len: 5, salt: "testsalt"]
    )

    field(:content, :string)
  end
end

defmodule EctoTablestore.TestSchema.TransactionTestRange do
  use EctoTablestore.Schema

  tablestore_schema "test_txn_range" do
    field(:key, :string, primary_key: true)
    field(:key2, :integer, primary_key: true)
    field(:field1, :string)
    field(:status, :integer)
  end
end

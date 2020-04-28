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
    field(:name, :string)
  end
end

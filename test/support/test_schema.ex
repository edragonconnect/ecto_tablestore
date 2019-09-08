defmodule EctoTablestore.TestSchema.Order do
  use EctoTablestore.Schema

  # primary_key supports `:id` and `:binary_id`

  tablestore_schema "ecto_ots_test_order" do
    field(:id, :binary_id, primary_key: true, autogenerate: false)
    field(:internal_id, :id, primary_key: true, autogenerate: true)
    field(:name, :string)
    field(:desc)
    field(:num, :integer)
    field(:success?, :boolean)
    field(:price, :float)
  end
end

defmodule EctoTablestore.TestSchema.User do
  use EctoTablestore.Schema

  tablestore_schema "ecto_ots_test_user" do
    field(:id, :id, primary_key: true)
    field(:name, :string)
    field(:level, :integer)
  end
end

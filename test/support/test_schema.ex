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

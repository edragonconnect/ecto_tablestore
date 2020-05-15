defmodule EctoTablestore.Support.Table do

  @instance EDCEXTestInstance

  alias EctoTablestore.TestSchema.{Order, User, User2, User3, Page}

  def create_order() do
    table = Order.__schema__(:source)
    ExAliyunOts.create_table(@instance, table,
      [{"id", :string}, {"internal_id", :auto_increment}])
  end

  def create_user() do
    table = User.__schema__(:source)
    ExAliyunOts.create_table(@instance, table,
      [{"id", :integer}])
  end

  def create_user2() do
    table = User2.__schema__(:source)
    ExAliyunOts.create_table(@instance, table,
      [{"id", :string}])
  end

  def create_user3() do
    table = User3.__schema__(:source)
    ExAliyunOts.create_table(@instance, table,
      [{"id", :string}])
  end

  def create_page() do
    table = Page.__schema__(:source)
    ExAliyunOts.create_table(@instance, table,
      [{"pid", :integer}, {"name", :string}])

    seq_name = Ecto.Adapters.Tablestore.bound_sequence_table_name(table)
    EctoTablestore.Sequence.create(@instance, seq_name)
  end

  def delete_order() do
    table = Order.__schema__(:source)
    ExAliyunOts.delete_table(@instance, table)
  end

  def delete_user() do
    table = User.__schema__(:source)
    ExAliyunOts.delete_table(@instance, table)
  end

  def delete_user2() do
    table = User2.__schema__(:source)
    ExAliyunOts.delete_table(@instance, table)
  end

  def delete_user3() do
    table = User3.__schema__(:source)
    ExAliyunOts.delete_table(@instance, table)
  end

  def delete_page() do
    table = Page.__schema__(:source)
    ExAliyunOts.delete_table(@instance, table)

    seq_name = Ecto.Adapters.Tablestore.bound_sequence_table_name(table)
    ExAliyunOts.delete_table(@instance, seq_name)
  end

end

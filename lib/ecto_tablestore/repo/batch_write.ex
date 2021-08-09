defmodule EctoTablestore.Repo.BatchWrite do
  @moduledoc false

  alias Ecto.Adapters.Tablestore
  alias ExAliyunOts.Const.OperationType

  require OperationType

  @adapter Tablestore

  def batch_write(repo, writes, options) do
    {_adapter, meta} = Ecto.Repo.Registry.lookup(repo)

    instance = meta.instance

    {write_requests, structs_map} =
      writes
      |> Enum.map(&map_batch_writes(instance, &1))
      |> Enum.unzip()


    {prepared_requests, opers} =
      write_requests
      |> reduce_merge_map()
      |> Enum.reduce({[], %{}}, fn {source, reqs}, {req_acc, oper_acc} ->
        opers =
          for req <- reqs do
            case req.type do
              OperationType.delete() -> :delete
              OperationType.update() -> :update
              OperationType.put() -> :put
            end
          end

        {
          [{source, reqs} | req_acc],
          Map.put(oper_acc, source, opers)
        }

      end)

    structs = reduce_merge_map(structs_map)

    result = ExAliyunOts.batch_write(instance, prepared_requests, options)

    case result do
      {:ok, response} ->
        {
          :ok,
          format_batch_write_response(response.tables, structs, opers)
        }

      _error ->
        result
    end
  end

  defp map_batch_writes(_instance, {:delete, deletes}) do
    Enum.reduce(deletes, {%{}, %{}}, fn delete, {delete_acc, acc} ->
      {source, pks, options, struct} = map_batch_write(:delete, delete) 
      prepared_request = ExAliyunOts.write_delete(pks, options)

      delete_acc = update_map_with_list(delete_acc, source, prepared_request)
      acc = update_map_with_list(acc, source, struct)
      {delete_acc, acc}
    end)
  end

  defp map_batch_writes(instance, {:put, puts}) do
    Enum.reduce(puts, {%{}, %{}}, fn put, {put_acc, acc} ->
      {source, pks, attrs, options, struct} = map_batch_write(:put, instance, put)
      prepared_request = ExAliyunOts.write_put(pks, attrs, options)

      put_acc = update_map_with_list(put_acc, source, prepared_request)
      acc = update_map_with_list(acc, source, struct)
      {put_acc, acc}
    end)
  end

  defp map_batch_writes(_instance, {:update, updates}) do
    Enum.reduce(updates, {%{}, %{}}, fn update, {update_acc, acc} ->
      {source, pks, options, struct} = map_batch_write(:update, update)
      prepared_request = ExAliyunOts.write_update(pks, options)

      update_acc = update_map_with_list(update_acc, source, prepared_request)
      acc = update_map_with_list(acc, source, struct)
      {update_acc, acc}
    end)
  end

  defp map_batch_writes(_instance, item) do
    raise("Invalid usecase - batch write with item: #{inspect(item)}")
  end

  defp map_batch_write(:delete, %{__meta__: _mate} = struct) do
    map_batch_write(:delete, {struct, []})
  end

  defp map_batch_write(:delete, {%{__meta__: meta} = struct, options}) do
    source = meta.schema.__schema__(:source)
    pks = Tablestore.primary_key_as_string(struct)
    options = Tablestore.generate_condition_options(struct, options)

    {source, pks, options, struct}
  end

  defp map_batch_write(:delete, {schema, pks}) do
    map_batch_write(:delete, {schema, pks, []})
  end

  defp map_batch_write(:delete, {schema, pks, options}) do
    source = schema.__schema__(:source)
    struct = struct(schema, pks)
    pks = Tablestore.primary_key_as_string(schema, pks)

    {source, pks, options, struct}
  end

  defp map_batch_write(:update, %Ecto.Changeset{valid?: true} = changeset) do
    map_batch_write(:update, {changeset, []})
  end

  defp map_batch_write(:update, {%Ecto.Changeset{valid?: true} = changeset, options}) do
    struct = changeset.data
    meta = struct.__meta__
    schema = meta.schema
    source = schema.__schema__(:source)
    dumper = schema.__schema__(:dump)

    pks = Tablestore.primary_key_as_string(struct)

    options = Tablestore.generate_condition_options(struct, options)

    embeds = schema.__schema__(:embeds)
    embeds = Ecto.Embedded.prepare(changeset, embeds, @adapter, :update)

    changes = Map.merge(changeset.changes, embeds)
    changes = dump_changes!(schema, changes, dumper)

    autogen_fields = Tablestore.autoupdate_fields(schema)
    changes = Keyword.merge(autogen_fields, changes)

    attrs = Tablestore.map_attrs_to_update(schema, changes)

    options = Keyword.merge(options, attrs)

    struct =
      changeset
      |> load_changes(embeds, autogen_fields)
      |> changeset_to_struct()

    {source, pks, options, struct}
  end

  defp map_batch_write(:put, instance, {schema, pks, attrs, options}) do
    struct = struct(schema, pks ++ attrs)
    map_batch_write(:put, instance, {Ecto.Changeset.change(struct), options})
  end

  defp map_batch_write(:put, instance, %{__meta__: _meta} = struct) do
    map_batch_write(:put, instance, {struct, []})
  end

  defp map_batch_write(:put, instance, {%{__meta__: _meta} = struct, options}) do
    map_batch_write(:put, instance, {Ecto.Changeset.change(struct), options})
  end

  defp map_batch_write(:put, instance, %Ecto.Changeset{valid?: true} = changeset) do
    map_batch_write(:put, instance, {changeset, []})
  end

  defp map_batch_write(:put, instance, {%Ecto.Changeset{valid?: true} = changeset, options}) do
    struct = changeset.data
    meta = changeset.data.__meta__
    schema = meta.schema
    source = schema.__schema__(:source)
    dumper = schema.__schema__(:dump)
    fields = schema.__schema__(:fields)

    # in put we always merge the whole struct into the changeset as changes.
    changeset = Ecto.Changeset.Relation.surface_changes(changeset, struct, fields)

    embeds = schema.__schema__(:embeds)
    embeds = Ecto.Embedded.prepare(changeset, embeds, @adapter, :insert)

    changes = 
      changeset.changes
      |> Map.merge(embeds)
      |> Map.take(fields)

    changes = dump_changes!(schema, changes, dumper)

    autogen_fields = Tablestore.autogen_fields(schema)
    changes = Keyword.merge(autogen_fields, changes)

    {pks, attrs, _autogenerate_id_name} = Tablestore.pks_and_attrs_to_put_row(instance, schema, changes)

    struct =
      changeset
      |> load_changes(embeds, autogen_fields)
      |> changeset_to_struct()

    options = Tablestore.generate_condition_options(:put, struct, options)

    {source, pks, attrs, options, struct}
  end

  defp map_batch_write(_, _instance, %Ecto.Changeset{valid?: false} = changeset) do
    raise "Using invalid changeset: #{inspect(changeset)} in batch writes"
  end

  defp map_batch_write(_, _instance, {%Ecto.Changeset{valid?: false} = changeset, _options}) do
    raise "Using invalid changeset: #{inspect(changeset)} in batch writes"
  end

  defp update_map_with_list(map, key, value) do
    Map.update(map, key, [value], fn current ->
      [value | current]
    end)
  end

  defp format_batch_write_response(tables_in_response, structs, opers) do
    Enum.reduce(tables_in_response, [], fn table, acc ->
      table_name = table.table_name

      structs_in_table = Map.get(structs, table_name)

      opers_in_table = Map.get(opers, table_name, [])

      results =
        Enum.zip(
          [opers_in_table, table.rows, structs_in_table]
        )
        |> Enum.reduce([], fn {oper, %{is_ok: is_ok, row: row} = response, struct}, oper_acc ->
          # `row` should be:
          #   pks only
          #   no-pks but with atomic increment result
          #   nil
          return =
            if is_ok == true do
              struct = Tablestore.row_to_struct(struct, row)
              {:ok, struct}
            else
              {:error, response}
            end

          Keyword.update(oper_acc, oper, [return], fn current ->
            [return | current]
          end)
        end)
      
      schema = List.first(structs_in_table).__meta__.schema
      Keyword.put(acc, schema, results)
    end)
  end

  defp changeset_to_struct(%{data: struct}) do
    struct
  end

  defp load_changes(changeset, embeds, autogen) do
    %{data: data, changes: changes} = changeset

    data =
      data
      |> merge_changes(changes)
      |> Map.merge(embeds)
      |> Map.merge(Map.new(autogen))

    Map.put(changeset, :data, data)
  end

  defp merge_changes(data, changes) do
    changes =
      Enum.reduce(changes, changes, fn {key, _value}, changes ->
        if Map.has_key?(data, key), do: changes, else: Map.delete(changes, key)
      end)

    Map.merge(data, changes)
  end

  defp reduce_merge_map(items) do
    Enum.reduce(items, %{}, fn map, acc ->
      Map.merge(acc, map, fn _key, list1, list2 ->
        List.flatten([list1 | list2])
      end)
    end)
  end

  defp dump_changes!(schema, changes, dumper) do
    for {field, value} <- changes do
      {_field, type} = Map.fetch!(dumper, field)
      {field, dump_field!(schema, type, field, value)}
    end
  end

  defp dump_field!(schema, type, field, value) do
    case Ecto.Type.adapter_dump(@adapter, type, value) do
      {:ok, value} ->
        value
      :error ->
        raise Ecto.ChangeError,
              "value `#{inspect(value)}` for `#{schema}.#{field}` does not match type #{inspect type}"
    end
  end

end

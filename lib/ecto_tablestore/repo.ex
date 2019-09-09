defmodule EctoTablestore.Repo do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Ecto.Repo,
        otp_app: Keyword.get(opts, :otp_app),
        adapter: Ecto.Adapters.Tablestore
    end
  end

  @doc """
  Returns the adapter tied to the repository.
  """
  @callback __adapter__ :: Ecto.Adapters.Tablestore.t()

  @doc """
  Similar to `c:get/3` but use schema entity which has been filled the whole primary key(s).

  Notice:

  If there are some attribute column(s) are provided in entity, these fields will be combined within multiple `:==` filtering expressions;
  If there are some attribute column(s) are provided and meanwhile set `filter` option, they will be merged into a composite filter.

  ## Options

  Please refer `c:get/3`.
  """
  @callback one(entity :: Ecto.Schema.t(), options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil

  @doc """
  Fetch a single struct from tablestore where the whole primary key(s) match the given ids.

  ## Options

  * `columns_to_get`, string list, return the specified attribute columns, if not specify this option field, will try to return all attribute columns together.
  * `start_column`, string, used as a starting column for Wide Column read, the return result contains this as starter.
  * `end_column`, string, used as a ending column for Wide Column read, the return result DON NOT contain this column.
  * `filter`, used as a filter by condition, support `">"`, `"<"`, `">="`, `"<="`, `"=="`, `"and"`, `"or"` and `"()"` expressions.

      The `ignore_if_missing` can be used for the non-existed attribute column, for example:

      An attribute column does not exist meanwhile set it as `true`, will ignore this match condition in the return result;

      An existed attribute column DOES NOT suit for this usecase, the match condition will always affect the return result, if match condition does not satisfy, they won't be
      return in result.

      ```elixir
      filter: filter(("name[ignore_if_missing: true]" == var_name and "age" > 1) or ("class" == "1"))
      ```

  * `transaction_id`, read under local transaction in a partition key.
  """
  @callback get(schema :: Ecto.Schema.t(), ids :: list, options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil

  @doc """
  Get multiple structs by range from one table, rely on the conjunction of the partition key and other primary key(s).

  ## Options

    * `direction`, set it as `:forward` to make the order of the query result in ascending by primary key(s), set it as `:backward` to make the order of the query result in descending by primary key(s).
    * `columns_to_get`, string list, return the specified attribute columns, if not specify this field all attribute columns will be return.
    * `start_column`, string, used as a starting column for Wide Column read, the return result contains this as starter.
    * `end_column`, string, used as a ending column for Wide Column read, the return result DON NOT contain this column.
    * `filter`, used as a filter by condition, support `">"`, `"<"`, `">="`, `"<="`, `"=="`, `"and"`, `"or"` and `"()"` expressions.

        The `ignore_if_missing` can be used for the non-existed attribute column, for example:

        An attribute column does not exist meanwhile set it as `true`, will ignore this match condition in the return result;

        An existed attribute column DOES NOT suit for this usecase, the match condition will always affect the return result, if match condition does not satisfy, they won't be
        return in result.

        ```elixir
        filter: filter(("name[ignore_if_missing: true]" == var_name and "age" > 1) or ("class" == "1"))
        ```

    * `transaction_id`, read under local transaction in a partition key.

  """
  @callback get_range(
              schema :: Ecto.Schema.t(),
              start_primary_keys :: list | binary(),
              end_primary_keys :: list,
              options :: Keyword.t()
            ) :: {list, nil} | {list, binary()} | {:error, term()}

  @doc """
  Batch get several rows of data from one or more tables, this batch request put multiple get_row in one request from client's perspective.
  """
  @callback batch_get(gets) ::
              {:ok, Keyword.t()} | {:error, term()}
            when gets: [
                   {
                     module :: Ecto.Schema.t(),
                     [{key :: String.t() | atom(), value :: integer | String.t()}],
                     options :: Keyword.t()
                   }
                   | {
                       module :: Ecto.Schema.t(),
                       [{key :: String.t() | atom(), value :: integer | String.t()}]
                     }
                   | (schema_entity :: Ecto.Schema.t())
                   | {[schema_entity :: Ecto.Schema.t()], options :: Keyword.t()}
                 ]

  @doc """
  Batch write several rows of data from one or more tables, this batch request put multiple put_row/delete_row/update_row in one request from client's perspective.
  After execute each operation in servers, return results independently and independently consumes capacity units.

  ## Example

  The options are similar as `put_row` / `delete_row` / `update_row`, but expect `transaction_id` option.

      batch_write([
        delete: [
          schema_entity_a,
          schema_entity_b
        ],
        put: [
          {%SchemaB{}, condition: condition(:ignore)},
          {%SchemaA{}, condition: condition(:expect_not_exist)}
        ],
        update: [
          {changeset_schema_a, return_type: :pk},
          {changeset_schema_b}
        ]
      ])

  """
  @callback batch_write(writes) ::
              {:ok, Keyword.t()} | {:error, term()}
            when writes: [
                   {
                     operation :: :put,
                     items :: [
                       schema_entity ::
                         Ecto.Schema.t()
                         | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
                         | {module :: Ecto.Schema.t(), ids :: list(), attrs :: list(),
                            options :: Keyword.t()}
                     ]
                   }
                   | {
                       operation :: :update,
                       items :: [
                         changeset ::
                           Ecto.Changeset.t()
                           | {changeset :: Ecto.Changeset.t(), options :: Keyword.t()}
                       ]
                     }
                   | {
                       operation :: :delete,
                       items :: [
                         schema_entity ::
                           Ecto.Schema.t()
                           | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
                           | {module :: Ecto.Schema.t(), ids :: list(), options :: Keyword.t()}
                       ]
                     }
                 ]

  @doc """
  Please see `c:Ecto.Repo.insert/2` for details.
  """
  @callback insert(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, term()}

  @doc """
  Please see `c:Ecto.Repo.delete/2` for details.
  """
  @callback delete(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, term()}

  @doc """
  Please see `c:Ecto.Repo.update/2` for details.
  """
  @callback update(
              changeset :: Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, term()}
end

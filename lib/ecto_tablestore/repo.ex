defmodule EctoTablestore.Repo do
  @moduledoc ~S"""
  Defines a repository for Tablestore.

  A repository maps to an underlying data store, controlled by `Ecto.Adapters.Tablestore` adapter.

  When used, the repository expects the `:otp_app` option, and uses `Ecto.Adapters.Tablestore` by
  default.  The `:otp_app` should point to an OTP application that has repository configuration.
  For example, the repository:

  ```elixir
  defmodule EctoTablestore.MyRepo do
    use EctoTablestore.Repo,
      otp_app: :my_otp_app
  end
  ```

  Configure `ex_aliyun_ots` as usual:

  ```elixir
  config :ex_aliyun_ots, MyInstance,
    name: "MY_INSTANCE_NAME",
    endpoint: "MY_INSTANCE_ENDPOINT",
    access_key_id: "MY_OTS_ACCESS_KEY",
    access_key_secret: "MY_OTS_ACCESS_KEY_SECRET"

  config :ex_aliyun_ots,
    instances: [MyInstance]
  ```

  Add the following configuration to associate `MyRepo` with the previous configuration of
  `ex_aliyun_ots`:

  ```elixir
  config :my_otp_app, EctoTablestore.MyRepo,
    instance: MyInstance
  ```
  """

  @type search_result :: %{
          is_all_succeeded: boolean(),
          next_token: binary() | nil,
          schemas: list(),
          total_hits: integer()
        }

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
  Provide search index features for the following scenarios:

  * MatchAllQuery
  * MatchQuery
  * MatchPhraseQuery
  * TermQuery
  * TermsQuery
  * PrefixQuery
  * RangeQuery
  * WildcardQuery
  * BoolQuery
  * NestedQuery
  * ExistsQuery
  * GeoBoundingBoxQuery
  * GeoDistanceQuery
  * GeoPolygonQuery

  Please refer [ExAliyunOts.Search](https://hexdocs.pm/ex_aliyun_ots/ExAliyunOts.Search.html#query) query section
  for details, they are similar options for use, for example:

  ```
  MyRepo.search(MySchema, "index_name",
    search_query: [
      query: exists_query("comment")
    ]
  )
  ```
  """
  @callback search(schema :: Ecto.Schema.t(), index_name :: String.t(), options :: Keyword.t()) ::
              {:ok, search_result} | {:error, term()}

  @doc """
  Similar to `c:get/3`, please ensure schema entity has been filled with the whole primary key(s).

  ## Options

    * `:entity_full_match`, whether to transfer the input attribute column(s) into the `:==`
      filtering expressions, by default it is `false`, when set `entity_full_match: true`, please
      notice the following rules:

       * If there exists attribute column(s) provided in entity, these fields will be combined
        within multiple `:==` filtering expressions;

       * If there exists attribute column(s) provided and meanwhile set `filter` option, they will
       be merged into a composite filter.

  Other options please refer `c:get/3`.
  """
  @callback one(entity :: Ecto.Schema.t(), options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil

  @doc """
  Fetch a single struct from tablestore where the whole primary key(s) match the given ids.

  ## Options

  * `:columns_to_get`, string list, return the specified attribute columns, if not specify this
    option field, will try to return all attribute columns together.
  * `:start_column`, string, used as a starting column for Wide Column read, the return result
    contains this as starter.
  * `:end_column`, string, used as a ending column for Wide Column read, the return result DON NOT
    contain this column.
  * `:filter`, used as a filter by condition, support `">"`, `"<"`, `">="`, `"<="`, `"=="`,
    `"and"`, `"or"` and `"()"` expressions.

      The `ignore_if_missing` option can be used for the non-existed attribute column, for
      example:

      An attribute column does not exist meanwhile set it as `true`, will ignore this match
      condition in the return result;

      An existed attribute column DOES NOT suit for this use case, the match condition will always
      affect the return result, if match condition does not satisfy, they won't be return in
      result.

      ```elixir
      filter: filter(({"name", ignore_if_missing: true} == var_name and "age" > 1) or ("class" == "1"))
      ```
  * `:transaction_id`, read under local transaction in a partition key.
  """
  @callback get(schema :: Ecto.Schema.t(), ids :: list, options :: Keyword.t()) ::
              Ecto.Schema.t() | {:error, term()} | nil

  @doc """
  Get multiple structs by range from one table, rely on the conjunction of the partition key and
  other primary key(s).

  ## Options

  * `:direction`, by default it is `:forward`, set it as `:forward` to make the order of the query
    result in ascending by primary key(s), set it as `:backward` to make the order of the query
    result in descending by primary key(s).
  * `:columns_to_get`, string list, return the specified attribute columns, if not specify this
    field all attribute columns will be return.
  * `:start_column`, string, used as a starting column for Wide Column read, the return result
    contains this as starter.
  * `:end_column`, string, used as a ending column for Wide Column read, the return result DON NOT
    contain this column.
  * `:limit`, optional, the maximum number of rows of data to be returned, this value must be
    greater than 0, whether this option is set or not, there returns a maximum of 5,000 data rows
    and the total data size never exceeds 4 MB.
  * `:transaction_id`, read under local transaction in a partition key.
  * `:filter`, used as a filter by condition, support `">"`, `"<"`, `">="`, `"<="`, `"=="`,
    `"and"`, `"or"` and `"()"` expressions.

      The `ignore_if_missing` option can be used for the non-existed attribute column, for
      example:

      An attribute column does not exist meanwhile set it as `true`, will ignore this match
      condition in the return result;

      An existed attribute column DOES NOT suit for this use case, the match condition will always
      affect the return result, if match condition does not satisfy, they won't be return in
      result.

      ```elixir
      filter: filter(({"name", ignore_if_missing: true} == var_name and "age" > 1) or ("class" == "1"))
      ```
  """
  @callback get_range(
              schema :: Ecto.Schema.t(),
              start_primary_keys :: list | binary(),
              end_primary_keys :: list,
              options :: Keyword.t()
            ) :: {nil, nil} | {list, nil} | {list, binary()} | {:error, term()}

  @doc """
  As a wrapper built on `ExAliyunOts.stream_range/5` to create composable and lazy enumerables
  stream for iteration.

  ## Options

  Please see options of `c:get_range/4` for details.
  """
  @callback stream_range(
              schema :: Ecto.Schema.t(),
              start_primary_keys :: list,
              end_primary_keys :: list,
              options :: Keyword.t()
            ) :: Enumerable.t()

  @doc """
  Batch get several rows of data from one or more tables, this batch request put multiple
  `get_row` in one request from client's perspective.

  After execute each operation in servers, return results independently and independently consumes
  capacity units.

  When input `schema_entity`, only theirs primary keys are used in query, if need to use theirs
  attribute columns into condition of query, please use `entity_full_match: true` option to do
  that.

  ## Example

      batch_get([
        {Schema1, [[ids: ids1], [ids: ids2]]},
        [%Schema2{keys: keys1}, %Schema2{keys: keys2}]
      ])

      batch_get([
        {Schema1, [[ids: ids1], [ids: ids2]]},
        {
          [
            %Schema2{keys: keys1},
            %Schema2{keys: keys2}
          ],
          entity_full_match: true
        }
      ])

      batch_get([
        {
          [
            %Schema2{keys: keys1},
            %Schema2{keys: keys2}
          ],
          filter: filter("attr_field" == 1),
          columns_to_get: ["attr_field", "attr_field2"]
        }
      ])

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
  Batch write several rows of data from one or more tables, this batch request put multiple
  put_row/delete_row/update_row in one request from client's perspective.

  After execute each operation in servers, return results independently and independently consumes
  capacity units.

  If use a batch write request include a transaction ID, all rows in that request can only be
  written to the table that matches the transaction ID.

  ## Options

    * `transaction_id`, use local transaction.

  ## Example

  The options of each `:put`, `:delete`, and `:update` operation are similar as
  `ExAliyunOts.put_row/5`, `ExAliyunOts.delete_row/4` and `ExAliyunOts.update_row/4`, but
  `transaction_id` option is using in the options of `c:EctoTablestore.Repo.batch_write/2`.

      batch_write([
        delete: [
          schema_entity_1,
          schema_entity_2
        ],
        put: [
          {%Schema2{}, condition: condition(:ignore)},
          {%Schema1{}, condition: condition(:expect_not_exist)},
          {changeset_schema_1, condition: condition(:ignore)}
        ],
        update: [
          {changeset_schema_1, return_type: :pk},
          {changeset_schema_2}
        ]
      ])
  """
  @callback batch_write(writes, options :: Keyword.t()) ::
              {:ok, Keyword.t()} | {:error, term()}
            when writes: [
                   {
                     operation :: :put,
                     items :: [
                       item ::
                         {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
                         | {module :: Ecto.Schema.t(), ids :: list(), attrs :: list(),
                            options :: Keyword.t()}
                         | {changeset :: Ecto.Changeset.t(), operation :: Keyword.t()}
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
  Inserts a struct defined via EctoTablestore.Schema or a changeset.

  ## Options

    * `:condition`, this option is required, whether to add conditional judgment before date insert.

      Two kinds of insert condition types as below:

      As `condition(:ignore)` means DO NOT do any condition validation before insert, if the schema
      non-partitioned primary key is auto increment, we can only use `condition(:ignore)` option.

      As `condition(:expect_not_exist)` means the primary key(s) are NOT existed before insert.  *
      `:transaction_id`, insert under local transaction in a partition key.

  """
  @callback insert(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              options :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, term()}

  @doc """
  Delete a struct using its primary key.

  ## Options

    * `:condition`, this option is required, whether to add conditional judgment before data
      delete.

    Two kinds of update condition types as below:

      As `condition(:expect_exist)` means the primary key(s) can match a row to delete, we also can add
      some compare expressions for the attribute columns, e.g.

        1. condition(:expect_exist, "attr1" == value1 and "attr2" > 1)
        2. condition(:expect_exist, "attr1" != value1)
        3. condition(:expect_exist, "attr1" > 100 or "attr2" < 1000)

      As `condition(:ignore)` means DO NOT do any condition validation before delete.

    * `:transaction_id`, delete under local transaction in a partition key.
    * `:stale_error_field` - The field where stale errors will be added in the returning
      changeset.  This option can be used to avoid raising `Ecto.StaleEntryError`.
    * `:stale_error_message` - The message to add to the configured `:stale_error_field` when
      stale errors happen, defaults to "is stale".
  """
  @callback delete(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              options :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, term()}

  @doc """
  Updates a changeset using its primary key.

  ## Options

    * `:condition`, this option is required, whether to add conditional judgment before data
      update.

      Two kinds of update condition types as below:

      As `condition(:expect_exist)` means the primary key(s) can match a row to update, we also can add
      some compare expressions for the attribute columns, e.g.

        1. condition(:expect_exist, "attr1" == value1 and "attr2" > 1)
        2. condition(:expect_exist, "attr1" != value1)
        3. condition(:expect_exist, "attr1" > 100 or "attr2" < 1000)

      As `condition(:ignore)` means DO NOT do any condition validation before update.

    * `:transaction_id`, update under local transaction in a partition key.
    * `:stale_error_field` - The field where stale errors will be added in the returning
      changeset.  This option can be used to avoid raising `Ecto.StaleEntryError`.
    * `:stale_error_message` - The message to add to the configured `:stale_error_field` when
      stale errors happen, defaults to "is stale".
  """
  @callback update(
              changeset :: Ecto.Changeset.t(),
              options :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, term()}

  @doc """
  Please see `c:Ecto.Repo.start_link/1` for details.
  """
  @callback start_link(options :: Keyword.t()) ::
              {:ok, pid}
              | {:error, {:already_started, pid}}
              | {:error, term}
end

defmodule EctoTablestore.Repo do
  @moduledoc ~S"""
  Defines a repository for Tablestore.

  A repository maps to an underlying data store, controlled by Ecto.Adapters.Tablestore adapter.

  When used, the repository expects the `:otp_app` option, and uses Ecto.Adapters.Tablestore by
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
          is_all_succeeded: boolean,
          next_token: binary | nil,
          schemas: list,
          total_hits: integer
        }
  @type schema :: Ecto.Schema.t()
  @type schema_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t()
  @type options :: Keyword.t()
  @type start_primary_keys :: list | binary
  @type end_primary_keys :: list
  @type index_name :: String.t()

  @typep batch_get_item ::
           {
             module :: Ecto.Schema.t(),
             [
               [{key :: String.t() | atom, value :: integer | String.t()}]
             ]
           }
           | {
               module :: Ecto.Schema.t(),
               [
                 [{key :: String.t() | atom, value :: integer | String.t()}]
               ],
               options :: Keyword.t()
             }
           | {
               module :: Ecto.Schema.t(),
               [{key :: String.t() | atom, value :: integer | String.t()}]
             }
           | {
               module :: Ecto.Schema.t(),
               [{key :: String.t() | atom, value :: integer | String.t()}],
               options :: Keyword.t()
             }
           | [schema_entity :: Ecto.Schema.t()]
           | {[schema_entity :: Ecto.Schema.t()], options :: Keyword.t()}
  @type batch_gets :: [batch_get_item]

  @typep batch_put_item ::
           Ecto.Schema.t()
           | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
           | {module :: Ecto.Schema.t(), ids :: list, attrs :: list, options :: Keyword.t()}
           | {changeset :: Ecto.Changeset.t(), operation :: Keyword.t()}

  @typep batch_update_item ::
           Ecto.Changeset.t()
           | {changeset :: Ecto.Changeset.t(), options :: Keyword.t()}

  @typep batch_delete_item ::
           Ecto.Schema.t()
           | {schema_entity :: Ecto.Schema.t(), options :: Keyword.t()}
           | {module :: Ecto.Schema.t(), ids :: list, options :: Keyword.t()}

  @type batch_writes :: [
          {operation :: :put, items :: [batch_put_item]}
          | {operation :: :update, items :: [batch_update_item]}
          | {operation :: :delete, items :: [batch_delete_item]}
        ]

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
  @callback search(schema, index_name, options) :: {:ok, search_result} | {:error, term}

  @doc """
  As a wrapper built on `ExAliyunOts.stream_search/4` to create composable and lazy enumerables
  stream for iteration.

  ## Options

  Please see options of `c:search/3` for details.
  """
  @callback stream_search(schema, index_name, options) :: Enumerable.t()

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
  @callback one(schema, options) :: schema | {:error, term} | nil

  @doc """
  See `c:one/2` for more details.
  """
  @callback one!(schema, options) :: schema | {:error, term} | nil

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
  @callback get(schema, ids :: list, options) :: schema | {:error, term} | nil

  @doc """
  Get multiple structs by range to the schema, result in `:forward` direction by default.

  ## Example

    Simply get result in a range request:

      Repo.get_range(Order)
      Repo.get_range(Order, direction: :backward)

    If the `Order` schema has multiple primary keys, and the `:id` field is the partition key,
    we can get records in range under the partition key `"1"` in this way:

      Repo.get_range(%Order{id: "1"})

  ## Options

  See `c:get_range/4`.
  """
  @callback get_range(schema, options) ::
              {nil, nil} | {list, nil} | {list, binary} | {:error, term}

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
  @callback get_range(schema, start_primary_keys, end_primary_keys, options) ::
              {nil, nil} | {list, nil} | {list, binary} | {:error, term}

  @doc """
  As a wrapper built on `stream_range/4` to create composable and lazy enumerables
  stream for iteration.

  ## Options

  Please see options of `c:get_range/4` for details.
  """
  @callback stream(schema, options) :: Enumerable.t()

  @doc """
  As a wrapper built on `ExAliyunOts.stream_range/5` to create composable and lazy enumerables
  stream for iteration.

  ## Options

  Please see options of `c:get_range/4` for details.
  """
  @callback stream_range(schema, start_primary_keys, end_primary_keys, options) :: Enumerable.t()

  @doc """
  Batch get several rows of data from one or more tables, this batch request put multiple
  `get_row` in one request from client's perspective.

  After execute each operation in servers, return results independently and independently consumes
  capacity units.

  When input `schema_entity`, only theirs primary keys are used in query, if need to use theirs
  attribute columns into condition of query, please use `entity_full_match: true` option to do
  that.

  ## Example

  When the schema has only 1 primary key, and you use default options:

      # get_row style
      batch_get([
        {Schema1, [[{"id", 1}], [{"id", 2}]]},
        {Schema2, [[{"id", 3}], [{"id", 4}]]}
      ])

      # get_row style
      # if only get one row from one schema could also be:
      batch_get([
        {Schema1, [{"id", 1}]},
        {Schema2, [{"id", 3}]}
      ])

      # schema_entity style
      batch_get([
        [%Schema1{id: 1}, %Schema1{id: 2}],
        [%Schema2{id: 3}, %Schema2{id: 4}]
      ])


  When the schema has only 1 primary key, and you use custom options:

      # get_row style
      batch_get([
        {Schema1, [{"id", 1}], [{"id", 2}], columns_to_get: ["field1"]},
        {Schema2, [{"id", 3}], [{"id", 4}], columns_to_get: ["field2"]}
      ])

      # schema_entity style
      batch_get([
        {
          [%Schema1{id: 1}, %Schema1{id: 2}],
          columns_to_get: ["field1"]
        },
        {
          [%Schema2{id: 3}, %Schema2{id: 4}],
          columns_to_get: ["field2"]
        }
      ])

  Entity full match:

      # only schema_entity style
      batch_get([
        {
          [%Schema1{id: 1, field1: 10}, %Schema1{id: 2, field1: 20}],
          entity_full_match: true
        },
        {
          [%Schema2{id: 3, field2: 30}, %Schema2{id: 4, field2: 40}],
          entity_full_match: true
        }
      ])

  When the schema has multiple primary keys:

      # get_row style
      batch_get([
        {Schema3, [
          [{"id", 1}, {"another_pk", "example1"}],
          [{"id", 2}, {"another_pk", "example2"}]
        ]},
        {Schema4, [
          [{"id", 3}, {"another_pk", "example3"}],
          [{"id", 4}, {"another_pk", "example4"}]
        ]}
      ])

      # schema_entity style
      batch_get([
        [
          %Schema3{id: 1, another_pk: "example1"},
          %Schema3{id: 2, another_pk: "example2"}
        ],
        [
          %Schema4{id: 3, another_pk: "example3"},
          %Schema4{id: 4, another_pk: "example4"}
        ]
      ])

  """
  @callback batch_get(batch_gets) :: {:ok, Keyword.t()} | {:error, term}

  @doc """
  Batch write several rows of data from one or more tables, this batch request puts multiple
  PutRow/DeleteRow/UpdateRow operations in one request from client's perspective.

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

  By default, require to explicitly set the `:condition` option in each operation, excepts that
  if a table defined an auto increment primary key(aka non-partitioned primary key) which is processed in the server side,
  the server logic MUST use `condition: condition(:ignore)`, in this case, this library internally forces to use
  `condition: condition(:ignore)`, so we can omit the `:condition` option in the PutRow operation of a batch write.

  If we set `entity_full_match: true` there will use the whole provided attribute-column field(s) of
  schema entity into the `column_condition` of condition filter, and always use `row_existence: :EXPECT_EXIST`,
  by default the `entity_full_match` option is `false`.

  If put a row with an auto increment primary key, meanwhile set `entity_full_match: true`, the `entity_full_match: true`
  option is no effect, this library internally forces to use `condition: condition(:ignore)`.

      batch_write([
        delete: [
          {schema_entity_1, condition: condition(:ignore)}
          {schema_entity_2, condition: condition(:expect_exist)}
        ],
        put: [
          {%Schema1{}, condition: condition(:expect_not_exist)},
          {%Schema2{}, condition: condition(:ignore)},
          {%Schema3WithAutoIncrementPK{}, return_type: :pk},
          {changeset_schema_1, condition: condition(:ignore)}
        ],
        update: [
          {changeset_schema_1, return_type: :pk},
          {changeset_schema_2, entity_full_match: true}
        ]
      ])

  Use `transaction_id` option:

      batch_write(
        [
          delete: [...],
          put: [...],
          update: [...]
        ],
        transaction_id: "..."
      )

  """
  @callback batch_write(batch_writes, options) :: {:ok, Keyword.t()} | {:error, term}

  @doc """
  Inserts a struct defined via EctoTablestore.Schema or a changeset.

  If a table defined an auto increment primary key which is processed in the server side, the server logic
  MUST use `condition(:ignore)`, in this case, this library internally forces to use `condition(:ignore)` to `:condition`
  option, so here we can omit the `:condition` option when insert a row.

  ## Options

    * `:condition`, this option is required excepts that the table defined an auto increment primary key,
      whether to add conditional judgment before date insert.

      1. As `condition(:ignore)` means DO NOT do any condition validation before insert, whether the row exists or not,
         both of the insert results will be success, if the schema non-partitioned primary key is auto increment,
         we can only use `condition(:ignore)` option.
      2. As `condition(:expect_not_exist)` means expect the primary key(s) are NOT existed before insert, if the row is
         existed, the insert result will be fail, if the row is not existed, the insert result will be success.
      3. As `condition(:expect_exist)` means overwrite the whole existed row to the primary key(s), if the row is existed,
         the insert result will be success, if the row is not existed, the insert result will be fail.

    * `:transaction_id`, insert under local transaction in a partition key.
  """
  @callback insert(schema_or_changeset, options) :: {:ok, schema} | {:error, term}

  @doc """
  Delete a struct using its primary key.

  ## Options

    * `:condition`, this option is required, whether to add conditional judgment before data
      delete.

        1. As `condition(:expect_exist)` means the primary key(s) can match a row to delete, if the row is existed, 
           the delete result will be success, if the row is not existed, the delete result will be fail.

             We also can add some compare expressions for the attribute columns, e.g:

               condition(:expect_exist, "attr1" == value1 and "attr2" > 1)
               condition(:expect_exist, "attr1" != value1)
               condition(:expect_exist, "attr1" > 100 or "attr2" < 1000)

        2. As `condition(:ignore)` means DO NOT do any condition validation before delete, whether the row exists or not,
           both of the delete results will be success.

        3. As `condition(:expect_not_exist)` means expect the primary key(s) are NOT existed before delete, if the row
           is existed, the delete result will be fail, if the row is not existed, the delete result will be success.

    * `:transaction_id`, delete under local transaction in a partition key.
    * `:stale_error_field` - The field where stale errors will be added in the returning
      changeset.  This option can be used to avoid raising `Ecto.StaleEntryError`.
    * `:stale_error_message` - The message to add to the configured `:stale_error_field` when
      stale errors happen, defaults to "is stale".
  """
  @callback delete(schema_or_changeset, options) :: {:ok, schema} | {:error, term}

  @doc """
  Updates a changeset using its primary key.

  ## Options

    * `:condition`, this option is required, whether to add conditional judgment before data
      update.

      1. As `condition(:expect_exist)` means the primary key(s) can match a row to update, if the row is existed,
         the update result will be success, if the row is not existed, the update result will be fail.

           We also can add some compare expressions for the attribute columns, e.g:

             condition(:expect_exist, "attr1" == value1 and "attr2" > 1)
             condition(:expect_exist, "attr1" != value1)
             condition(:expect_exist, "attr1" > 100 or "attr2" < 1000)

      2. As `condition(:ignore)` means DO NOT do any condition validation before update, whether the row exists or not,
         both of the update results will be success.

      3. As `condition(:expect_not_exist)` means expect the primary key(s) are NOT existed before update, if the row
         is existed, the update will be fail, if the row is not existed, the update result will be success.

    * `:transaction_id`, update under local transaction in a partition key.
    * `:stale_error_field` - The field where stale errors will be added in the returning
      changeset. This option can be used to avoid raising `Ecto.StaleEntryError`.
    * `:stale_error_message` - The message to add to the configured `:stale_error_field` when
      stale errors happen, defaults to "is stale".
    * `:returning`, this option is required when the input changeset with `:increment` operation, all fields of the atomic
      increment operation are required to explicitly set into this option in any order, if missed any atomic increment
      operation related field, there will raise an `Ecto.ConstraintError` to prompt and terminate this update.
      If there is no `:increment` operation, the `:returning` option is no need to set. If set `returning: true`, but not
      really all fields are changed, the unchanged fields will be replaced as `nil` in the returned schema data.
  """
  @callback update(changeset :: Ecto.Changeset.t(), options) :: {:ok, schema} | {:error, term}

  @doc """
  Please see `c:Ecto.Repo.start_link/1` for details.
  """
  @callback start_link(options) :: {:ok, pid} | {:error, {:already_started, pid}} | {:error, term}
end

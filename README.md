# Alibaba Tablestore adapter for Ecto

[![Module Version](https://img.shields.io/hexpm/v/ecto_tablestore.svg)](https://hex.pm/packages/ecto_tablestore)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/ecto_tablestore/)
[![Total Download](https://img.shields.io/hexpm/dt/ecto_tablestore.svg)](https://hex.pm/packages/ecto_tablestore)
[![License](https://img.shields.io/hexpm/l/ecto_tablestore.svg)](https://github.com/edragonconnect/ecto_tablestore/blob/master/LICENSE.md)
[![Last Updated](https://img.shields.io/github/last-commit/edragonconnect/ecto_tablestore.svg)](https://github.com/edragonconnect/ecto_tablestore/commits/master)

## Notice

If you have used this library and upgrade it into `0.11.0` or later, there are some breaking changes in `0.11.0`,
you may need to make a data migration before upgrade and one use case check, please see this
[changelog](https://github.com/edragonconnect/ecto_tablestore/blob/master/CHANGELOG.md)
for details, if you are new to use `0.11.0` or later, please ignore this comment.

## Introduce

Ecto 3.x adapter for [Alibaba Tablestore](https://www.alibabacloud.com/product/table-store), this
is built on top of [`ex_aliyun_ots`](https://hex.pm/packages/ex_aliyun_ots) to implement
`Ecto.Adapter` and `Ecto.Adapter.Schema` behaviours.

Supported features:

* Compatible `Ecto.Repo` API.

* Support schema's `timestamps()` macro, make `inserted_at` and `updated_at` as an integer UTC
  timestamps.

* Support `:map` | `{:map, _}` | `:array` | `{:array, _}` field type, use Jason to encode the
  field value into :string when save, use Jason to simply decode the field value into `:map` |
  `:array` when read, the :keys option of Jason's decode always use :string.

* Support the partition key is autoincrementing, use the sequence feature provided by
  `ex_aliyun_ots`.

* Automatically converts the returned original row results into corresponding schema(s).

* Automatically generate the provided attribute-column field(s) of schema entity into the `filter`
  expression option of `GetRow` (see `c:EctoTablestore.Repo.one/2`) and `BatchGet` (see
  `c:EctoTablestore.Repo.batch_get/1`) when use `entity_full_match: true`, by default this option is
  `false`.

* Automatically generate the provided attribute-column field(s) of schema entity into the
  `condition` expression option of `BatchWrite` (see `c:EctoTablestore.Repo.batch_write/2`) when
  use `entity_full_match: true`, by default this option is `false`.

* Automatically map changeset's attribute-column field(s) into `UpdateRow` operation when call
  `c:EctoTablestore.Repo.update/2`:

  * Use atomic increment via `{:increment, integer()}` in changeset, and return the increased
    value in the corresponding field(s) by default;
  * Set any attribute(s) of schema changeset as `nil` will `:delete_all` that attribute-column
    field(s);
  * Set existed attribute(s) in schema changeset will `:put` to save.

* Support embedded schema, please refer full functions of `Ecto.Schema` for details.

* Automatically use schema defined attribute fields into `:columns_to_get` in all read operations
  by default if this option is not explicitly provided.

Implement Tablestore row related functions in `EctoTablestore.Repo` module, please see
[document](https://hexdocs.pm/ecto_tablestore/readme.html) for details:

* PutRow
* GetRow
* UpdateRow
* DeleteRow
* GetRange
* StreamRange
* BatchGetRow
* BatchWriteRow
* Search

## Migration

Provide a simple migration to create or drop the table, the secondary index and the search index, please see
`EctoTablestore.Migration` for details.

## Usage

1, Configure `My` instance(s) information of Alibaba Tablestore product.

```elixir
use Mix.Config

# config for `ex_aliyun_ots`

config :ex_aliyun_ots, MyInstance,
  name: "MY_INSTANCE_NAME",
  endpoint: "MY_INSTANCE_ENDPOINT",
  access_key_id: "MY_OTS_ACCESS_KEY",
  access_key_secret: "MY_OTS_ACCESS_KEY_SECRET"

config :ex_aliyun_ots,
  instances: [MyInstance]

# config for `ecto_tablestore`

config :my_otp_app, EctoTablestore.MyRepo,
  instance: MyInstance
```

2, Create the `EctoTablestore.MyRepo` module mentioned earlier in the configuration, use
`EctoTablestore.Repo` and set required `otp_app` option with your OTP application's name.

```elixir
defmodule EctoTablestore.MyRepo do
  use EctoTablestore.Repo,
    otp_app: :my_otp_app
end
```

3, Each repository in Ecto defines a `start_link/0` function that needs to be invoked before using
the repository. In general, this function is not called directly, but used as part of your
application supervision tree.

Add `EctoTablestore.MyRepo` into your application start callback that defines and start your
supervisor. You just need to edit `start/2` function to start the repo as a supervisor on your
application's supervisor:

```elixir
def start(_type, _args) do
  children = [
    {EctoTablestore.MyRepo, []}
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

4, After finish the above preparation, we can use `MyRepo` to operate data store.

## Integrate Hashids

Base on unique integer of atomic-increment sequence, provides a way to simply integrate `Hashids`
to generate your *hash* ids when insert row(s), for `Repo.insert/2` or `Repo.batch_write/1`.

### Use in schema

```elixir
defmodule Module do
  use EctoTablestore.Schema

  schema "table_name" do
    field(:id, Ecto.Hashids, primary_key: true, autogenerate: true,
      hashids: [salt: "123", min_len: 2, alphabet: "..."])
    field(:content, :string)
  end

end
```

Options:

* The `primary_key` as true is required;
* The `autogenerate` as true is required;
* The `salt`, `min_len` and `alphabet` of hashids options are used for configuration options from
  `Hashids.new/1`.

Notice: the `salt`, `min_len` and `alphabet` of hashids options also can be configured in the `:ecto_tablestore`
config for each defined schema, for example:

```elixir
config :ecto_tablestore,
  hashids: [
    {Module, salt: "...", min_len: 16, alphabet: "..."},
    ...
  ]
```

### Use in migration

Use `:hashids` as a type in `add` operation to define the partition key, the principle behind this
will use `ecto_tablestore_default_seq` as a global default table to maintain the sequences of all
tables, if `ecto_tablestore_default_seq` is not existed, there will create this, if it is existed,
please ignore the "OTSObjectAlreadyExist" error of requested table already exists.

```elixir
defmodule EctoTablestore.TestRepo.Migrations.TestHashids do
  use EctoTablestore.Migration

  def change do
    create table("table_name") do
      add :id, :hashids, partition_key: true
      add :oid, :integer
    end
  end

end
```

Options:

* The `partition_key` option as true is required;
* The `auto_increment` option is ignored when the partition key is `:hashids` type.

## References

Alibaba Tablestore product official references:

* [English document](https://www.alibabacloud.com/help/doc-detail/27280.htm)
* [中文文档](https://help.aliyun.com/document_detail/27280.html)

## License

This project is licensed under the MIT license. Copyright (c) 2018- Xin Zou.

# Changelog

## v0.15.0 (2024-01-22)

* Implement `SQL` query ([#40](https://github.com/edragonconnect/ecto_tablestore/pull/40)).
* Implement `Ecto.Adapter.Transaction` ([#41](https://github.com/edragonconnect/ecto_tablestore/pull/41))

## v0.14.0 (2023-01-04)

* Add `Repo.one!/2`.
* Enhance `Repo.get_range/2`, `Repo.stream/2` with `token` option.
* Enhance output when create search index.

## v0.13.3 (2022-05-24)

* Fix and update document to Repo's batch get.
* Fix to use the public API `Ecto.Adapter.lookup_meta/1` when upgrade to `ecto ~> 3.8.*`.

## v0.13.2 (2022-01-21)

* Fix schema's timestamps() as as the original with second time unit.
* Fix `Repo.get_range/4` document missed.

## ~~v0.13.1 (2021-10-25)~~

* Update document to clarify `EctoTablestore.Schema` use.
* Some changes to schema's timestamps() but with unexpected change the time unit of `inserted_at`/`updated_at` fields from second to nanosecond, fixed in `0.13.2`, so please DO NOT use this version.

## v0.13.0 (2021-08-25)

* Refactor `:hashids` type with Ecto.ParameterizedType.
* Clean logger when create sequence.
* Fix warning: function checked_out?/1 required with ecto `3.7.0`.

## v0.12.2 (2021-08-20)

* Some fix and enhancement via [#34](https://github.com/edragonconnect/ecto_tablestore/pull/34):

  1. Fix to properly adapt Ecto type on load;
  2. Add support to `:decimal` type in the field of schema;
  3. Add `Ecto.ReplaceableString` type.

## v0.12.1 (2021-08-11)

* Use schema defined attribute fields into `:columns_to_get` option as default
  in all read operations via [#33](https://github.com/edragonconnect/ecto_tablestore/pull/33).

## v0.12.0 (2021-08-10)

* Some fix and enhancement via [#32](https://github.com/edragonconnect/ecto_tablestore/pull/32):

  1. Fix fail when batch write with `embeds_many` or `embeds_one` fields;
  2. Add `:entity_full_match` as an optional option to leverage the provided attribute columns of the struct into the filter of the batch write
     condition when it is `true`, default value is `false`. After this breaking change, each put/update/delete operation of batch write requires
     explicitly set `:condition` option, excepts the following `#3` case;
  3. When insert or a `:put` operation of batch write with a table defined an auto increment primary key(a server side function), we can omit
     `:condition` option, because the server side only accepts `condition: condition(:ignore)`, we internally wrapper this to simplify the
     input in this use case;
  4. Clarify document about the `:condition` option of insert/update/delete/batch_write;
  5. Some code naming improvement.

## v0.11.2 (2021-05-21)

* Add `drop_if_exists/1` function for migration.
* Add `--migrations_path` option to `mix ecto.ots.migrate`.

## v0.11.1 (2021-05-17)

* Fix format error to put the `.formatter.exs` file into package.

## v0.11.0 (2021-05-14)

* [Need to migrate before upgrade] When define the type of partition key as an auto increment integer, we
  created a `"#{table_name}_seq"` table as an internal sequence to the `"#{table_name}"` table,
  but now all sequence(s) managed into the `"ecto_tablestore_default_seq"` table, see
  [#16](https://github.com/edragonconnect/ecto_tablestore/pull/16).
* Add `Repo.stream_search/3`, see [#17](https://github.com/edragonconnect/ecto_tablestore/pull/16).
* Add `Repo.stream/2`, see [#21](https://github.com/edragonconnect/ecto_tablestore/pull/21).
* Add `Repo.get_range/1` and `Repo.get_range/2` for easy use, see
  [#28](https://github.com/edragonconnect/ecto_tablestore/pull/28).
* Support changeset optimistic_lock in update, see commit
  [66f6823](https://github.com/edragonconnect/ecto_tablestore/commit/66f6823704f14940e97f8195e63ad2c29b77ecea).
* [Need to check before upgrade] Fix update with `:increment` operation return an unexpected result when
  schema has multi integer fields defined, and require to explicitly add column name(s) which are related in the atomic increment
  operation into the `:returning` option of `Repo.update/2`,
  see [#19](https://github.com/edragonconnect/ecto_tablestore/pull/19).
* Fix to properly handle the stale error case when insert,
  see [#20](https://github.com/edragonconnect/ecto_tablestore/pull/20).
* Fix to remove the useless supervisor name to resolve duplicated naming conflict in some cases,
  see [#24](https://github.com/edragonconnect/ecto_tablestore/pull/24).

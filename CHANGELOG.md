# Changelog

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

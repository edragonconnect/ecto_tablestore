# Alibaba Tablestore adapter for Ecto

Ecto 3.x adapter for Alibaba Tablestore, this is based on [`ex_aliyun_ots`](https://hex.pm/packages/ex_aliyun_ots) build to implement `Ecto.Adapter` and `Ecto.Adapter.Schema` behaviours.

Supported features:

* Compatible `Ecto.Repo` API.
* Automatically converts the returned original row results into corresponding schema(s).
* Automatically generate the provided attribute-column field(s) of schema entity into the `filter` expression option of `GetRow` (see `EctoTablestore.Repo.one/2`) and `BatchGet` (see `EctoTablestore.Repo.batch_get/1`).
* Automatically generate the provided attribute-column field(s) of schema entity into the `condition` expression option of `BatchWrite`.
* Automatically map changeset's attribute-column field(s) into `UpdateRow` operation when call `EctoTablestore.Repo.update/2`:
  * Use atomic increment via `{:increment, integer()}` in changeset, and return the increased value in the corrsponding field(s) by default;
  * Set any attribute(s) of schema changeset as `nil` will `:delete_all` that attribute-column field(s);
  * Set existed attribute(s) in schema changeset will `:put` to save.

Implement Tablestore row related functions in `EctoTablestore.Repo` module, please see document for details:

* PutRow
* GetRow
* UpdateRow
* DeleteRow
* GetRange
* BatchGetRow
* BatchWriteRow
* Search

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

2, Create the `EctoTablestore.MyRepo` module mentioned earlier in the configuration, use `EctoTablestore.Repo` and set required `otp_app` option with your OTP application's name.

```elixir
defmodule EctoTablestore.MyRepo do
  use EctoTablestore.Repo,
    otp_app: :my_otp_app
end
```

3, Each repository in Ecto defines a `start_link/0` function that needs to be invoked before using the repository. In general, this function is not called directly, but used as
part of your application supervision tree.

Add `EctoTablestore.MyRepo` into your application start callback that defines and start your supervisor. You just need to edit `start/2` function to start the repo as a
supervisor on your application's supervisor:

```elixir
def start(_type, _args) do
  children = [
    {EctoTablestore.MyRep, []}
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

## References

Alibaba Tablestore product official references:

* [English document](https://www.alibabacloud.com/help/doc-detail/27280.htm)
* [中文文档](https://help.aliyun.com/document_detail/27280.html)

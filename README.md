# EctoTablestore

**TODO: Add description**

## Introduce

Alibaba Tablestore product official references:

* [English document](https://www.alibabacloud.com/help/doc-detail/27280.htm)
* [中文文档](https://help.aliyun.com/document_detail/27280.html)

## Config

1, Configure `MY` instance(s) information of Alibaba Tablestore product.

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

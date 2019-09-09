# EctoTablestore

**TODO: Add description**

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

2, Create the `EctoTablestore.MyRepo` module mentioned earlier in the configuration, and use `EctoTablestore.Repo` with `:my_otp_app` option to finish the setup.

```elixir
defmodule EctoTablestore.MyRepo do
  use EctoTablestore.Repo,
    otp_app: :my_otp_app
end
```

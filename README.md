# EctoTablestore

**TODO: Add description**

## Config

1, Configure `YOUR` instance(s) information of Alibaba Tablestore product.

```elixir
use Mix.Config

# config for `ex_aliyun_ots`

config :ex_aliyun_ots, YourInstance,
  name: "YOUR_INSTANCE_NAME",
  endpoint: "YOUR_INSTANCE_ENDPOINT",
  access_key_id: "YOUR_OTS_ACCESS_KEY",
  access_key_secret: "YOUR_OTS_ACCESS_KEY_SECRET"

config :ex_aliyun_ots,
  instances: [YourInstance]
  
# config for `ecto_tablestore`

config :ecto_tablestore, EctoTablestore.YourRepo,
  instance: YourInstance

```

2, Create the `EctoTablestore.YourRepo` module mentioned earlier in the configuration, and `use EctoTablestore.Repo` to finish the setup.

```elixir
defmodule EctoTablestore.YourRepo do
  use EctoTablestore.Repo
end
```

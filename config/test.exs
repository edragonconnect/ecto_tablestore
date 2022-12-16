import Config

config :ex_aliyun_ots, instances: [EDCEXTestInstance]

config :ex_aliyun_ots, EDCEXTestInstance,
  name: "instance_name",
  endpoint: "https://instance_name.cn-shenzhen.ots.aliyuncs.com",
  access_key_id: "access_key_id",
  access_key_secret: "access_key_secret"

config :ecto_tablestore, EctoTablestore.TestRepo, instance: EDCEXTestInstance

config :ecto_tablestore,
  ecto_repos: [EctoTablestore.TestRepo],
  hashids: [
    {EctoTablestore.TestSchema.Post, salt: "123", min_len: 2}
  ]

import_config "test.secret.exs"

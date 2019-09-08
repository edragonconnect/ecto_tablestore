defmodule EctoTablestore.Repo do
  use Ecto.Repo,
    otp_app: :ecto_tablestore,
    adapter: Ecto.Adapters.Tablestore
end

defmodule EctoTablestore.Repo do

  defmacro __using__(_opts) do
    quote do
      use Ecto.Repo,
        otp_app: :ecto_tablestore,
        adapter: Ecto.Adapters.Tablestore
    end
  end

end

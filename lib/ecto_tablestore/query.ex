defmodule EctoTablestore.Query do
  @moduledoc false

  import ExAliyunOts, only: [condition: 2, filter: 1]

  def condition(existence) do
    ExAliyunOts.condition(existence)
  end

  defmacro condition(existence, expr) do
    quote do
      condition(unquote(existence), unquote(expr))
    end
  end

  defmacro filter(filter_expr) do
    quote do
      filter(unquote(filter_expr))
    end
  end
end

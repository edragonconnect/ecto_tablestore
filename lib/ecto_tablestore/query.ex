defmodule EctoTablestore.Query do
  @moduledoc false

  import ExAliyunOts, only: [condition: 1, condition: 2, filter: 1]

  defmacro condition(existence) do
    quote do
      condition(unquote(existence))
    end
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

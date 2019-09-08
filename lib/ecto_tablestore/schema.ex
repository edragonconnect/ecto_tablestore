defmodule EctoTablestore.Schema do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      use Ecto.Schema

      import EctoTablestore.Schema, only: [tablestore_schema: 2]

      @primary_key false
    end
  end

  defmacro tablestore_schema(source, do: block) do
    block = check_block(block)

    quote do
      Ecto.Schema.schema(unquote(source), do: unquote(block))
    end
  end

  defp check_block({:__block__, info, fields}) do
    {:__block__, info, supplement_fields(fields, [])}
  end

  defp supplement_fields([], prepared) do
    Enum.reverse(prepared)
  end

  defp supplement_fields(
         [{defined_macro, field_line, [field_name, :integer]} | rest_fields],
         prepared
       ) do
    update = {
      defined_macro,
      field_line,
      [
        field_name,
        {:__aliases__, field_line, [:EctoTablestore, :Integer]},
        [read_after_writes: true]
      ]
    }

    supplement_fields(rest_fields, [update | prepared])
  end

  defp supplement_fields(
         [{defined_macro, field_line, [field_name, :integer, opts]} | rest_fields],
         prepared
       ) do
    update = {
      defined_macro,
      field_line,
      [
        field_name,
        {:__aliases__, field_line, [:EctoTablestore, :Integer]},
        Keyword.put(opts, :read_after_writes, true)
      ]
    }

    supplement_fields(rest_fields, [update | prepared])
  end

  defp supplement_fields(
         [
           {defined_macro, field_line,
            [field_name, {:__aliases__, line, [:EctoTablestore, :Integer]}]}
           | rest_fields
         ],
         prepared
       ) do
    update = {
      defined_macro,
      field_line,
      [field_name, {:__aliases__, line, [:EctoTablestore, :Integer]}, [read_after_writes: true]]
    }

    supplement_fields(rest_fields, [update | prepared])
  end

  defp supplement_fields(
         [
           {defined_macro, field_line,
            [field_name, {:__aliases__, line, [:EctoTablestore, :Integer]}, opts]}
           | rest_fields
         ],
         prepared
       ) do
    update = {
      defined_macro,
      field_line,
      [
        field_name,
        {:__aliases__, line, [:EctoTablestore, :Integer]},
        Keyword.put(opts, :read_after_writes, true)
      ]
    }

    supplement_fields(rest_fields, [update | prepared])
  end

  defp supplement_fields([{defined_macro, field_line, field_info} | rest_fields], prepared) do
    supplement_fields(rest_fields, [{defined_macro, field_line, field_info} | prepared])
  end
end

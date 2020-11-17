defmodule EctoTablestore.Sequence do
  @moduledoc false
  require Logger
  alias ExAliyunOts.{Var, Sequence}

  @default_seq "ecto_tablestore_default_seq"

  def default_table, do: @default_seq

  def create(instance, seq_name \\ @default_seq) do
    new_seq = %Var.NewSequence{name: seq_name}
    result = Sequence.create(instance, new_seq)
    Logger.info("create a sequence: #{seq_name} result: #{inspect(result)}")
    result
  end

  def next_value(instance, seq_name, field_name)
      when is_bitstring(seq_name) and is_bitstring(field_name) do
    var_next = %Var.GetSequenceNextValue{
      name: seq_name,
      event: field_name
    }

    Sequence.next_value(instance, var_next)
  end

  @doc """
  Use a global sequence table to store all kinds of sequences to the tables.
  """
  def next_value(instance, event)
      when is_bitstring(event) do
    var_next = %Var.GetSequenceNextValue{
      name: @default_seq,
      event: event
    }

    Sequence.next_value(instance, var_next)
  end
end

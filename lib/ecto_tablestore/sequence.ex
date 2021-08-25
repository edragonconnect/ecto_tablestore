defmodule EctoTablestore.Sequence do
  @moduledoc false

  alias ExAliyunOts.{Var, Sequence}

  @default_seq "ecto_tablestore_default_seq"

  def default_table, do: @default_seq

  def create(instance, seq_name \\ @default_seq) do
    Sequence.create(instance, %Var.NewSequence{name: seq_name})
  end

  def next_value(instance, seq_name, field_name)
      when is_bitstring(seq_name) and is_bitstring(field_name) do
    Sequence.next_value(instance, %Var.GetSequenceNextValue{name: seq_name, event: field_name})
  end

  @doc """
  Use a global sequence table to store all kinds of sequences to the tables.
  """
  def next_value(instance, event) when is_bitstring(event) do
    Sequence.next_value(instance, %Var.GetSequenceNextValue{
      name: @default_seq,
      event: event
    })
  end
end

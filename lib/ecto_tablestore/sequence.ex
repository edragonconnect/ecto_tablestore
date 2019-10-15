defmodule EctoTablestore.Sequence do
  @moduledoc false

  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Sequence

  def create(instance, seq_name) do
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
end

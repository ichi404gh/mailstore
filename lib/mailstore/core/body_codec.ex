defmodule Mailstore.Core.BodyCodec do
  @moduledoc """
  Body compression. zstd ships with OTP 28+. Next step: per-class trained
  dictionaries (warmup texts are near-identical, small messages gain 3-5x).
  """
  @raw 0
  @zstd 1
  @min_size 256

  def encode(raw) when byte_size(raw) < @min_size, do: {@raw, raw}

  def encode(raw) do
    compressed = raw |> :zstd.compress() |> IO.iodata_to_binary()
    if byte_size(compressed) < byte_size(raw), do: {@zstd, compressed}, else: {@raw, raw}
  end

  def decode(@raw, data), do: data
  def decode(@zstd, data), do: data |> :zstd.decompress() |> IO.iodata_to_binary()
end

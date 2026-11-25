defmodule Mailstore.Core.BodyCodecTest do
  use ExUnit.Case, async: true
  alias Mailstore.Core.BodyCodec

  test "zstd round-trips and compresses repetitive mail" do
    raw = String.duplicate("Hi Bob, just following up. ", 80)
    {codec, data} = BodyCodec.encode(raw)
    assert codec == 1 and byte_size(data) < byte_size(raw) / 5
    assert BodyCodec.decode(codec, data) == raw
  end

  test "tiny bodies are stored raw" do
    assert BodyCodec.encode("tiny") == {0, "tiny"}
  end
end

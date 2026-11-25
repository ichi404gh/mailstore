defmodule Mailstore.Core.MessageClass do
  @moduledoc """
  What kind of mail this is. Set by the upstream classifier before delivery; decides
  routing (warmup never goes to the master inbox), retention and full-text indexing.
  """
  @classes [other: 0, warmup: 1, reply: 2, bounce: 3, sent: 4, spam_test: 5]
  @by_int Map.new(@classes, fn {k, v} -> {v, k} end)

  def all, do: Keyword.keys(@classes)
  def to_int(class), do: Keyword.fetch!(@classes, class)
  def from_int(int), do: Map.fetch!(@by_int, int)
end

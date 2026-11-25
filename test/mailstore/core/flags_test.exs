defmodule Mailstore.Core.FlagsTest do
  use ExUnit.Case, async: true
  alias Mailstore.Core.Flags

  test "flags round-trip, keywords sorted and unique" do
    assert {9, ["$Forwarded", "work"]} =
             Flags.parse(["\\Seen", "work", "\\deleted", "$Forwarded", "work", "\\Recent"])

    assert Flags.to_list(9, ["work"]) == ["\\Seen", "\\Deleted", "work"]
  end
end

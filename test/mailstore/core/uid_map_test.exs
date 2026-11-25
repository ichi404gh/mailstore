defmodule Mailstore.Core.UidMapTest do
  @moduledoc "Range-based UID map checked against a plain sorted list on random operations."
  use ExUnit.Case, async: true
  alias Mailstore.Core.UidMap
  alias Mailstore.Imap.SequenceSet

  test "matches a plain list model under random appends, insertions and removals" do
    :rand.seed(:exsss, {1, 2, 3})

    kinds =
      Enum.reduce(1..300, {UidMap.new([]), [], MapSet.new()}, fn _step, {map, model, kinds} ->
        r = :rand.uniform()

        {map, model} =
          cond do
            r < 0.45 or model == [] ->
              # append a batch of new UIDs, sometimes with gaps
              start = Enum.max([0 | model]) + :rand.uniform(3)
              # new mail is usually a contiguous batch; sometimes leave gaps
              new =
                if :rand.uniform() < 0.5,
                  do: Enum.to_list(start..(start + :rand.uniform(6))),
                  else: Enum.take_random(start..(start + 10), :rand.uniform(6))

              {UidMap.append(map, new), Enum.sort(Enum.uniq(model ++ new))}

            r < 0.65 ->
              # put UIDs anywhere, some already present (like \\Seen being removed again)
              holes = Enum.to_list(1..Enum.max(model)) -- model
              new = Enum.take_random(holes ++ Enum.take_random(model, 2), :rand.uniform(5))
              {UidMap.put(map, new), Enum.sort(Enum.uniq(model ++ new))}

            true ->
              gone = Enum.take_random(model, :rand.uniform(min(4, length(model)))) ++ [999_999]
              expected_seqs = for {uid, i} <- Enum.with_index(model, 1), uid in gone, do: i
              {seqs, map} = UidMap.remove(map, gone)
              assert seqs == Enum.reverse(expected_seqs)
              {map, model -- gone}
          end

        assert UidMap.to_list(map) == model
        assert UidMap.size(map) == length(model)
        assert UidMap.max_uid(map) == Enum.max([0 | model])

        for {uid, seq} <- Enum.with_index(model, 1) do
          assert UidMap.uid(map, seq) == uid
          assert UidMap.seq(map, uid) == seq
        end

        assert UidMap.uid(map, length(model) + 1) == nil
        assert UidMap.seq(map, 0) == nil

        # ranges are maximal: never adjacent
        for [{_, b}, {c, _}] <- Enum.chunk_every(UidMap.ranges(map), 2, 1, :discard),
            do: assert(c > b + 1)

        # uids_between and UID sets agree with the model
        {lo, hi} = Enum.min_max([:rand.uniform(80), :rand.uniform(80)])
        assert UidMap.uids_between(map, lo, hi) == Enum.filter(model, &(&1 >= lo and &1 <= hi))
        {:ok, set} = SequenceSet.parse("#{lo}:#{hi},#{hi}:*")
        max = Enum.max([0 | model])

        assert SequenceSet.uids(set, map) ==
                 Enum.filter(model, &SequenceSet.member?(set, &1, max))

        {map, model, MapSet.put(kinds, map.kind)}
      end)
      |> elem(2)

    # the random walk exercises both representations
    assert kinds == MapSet.new([:ranges, :plain])
  end

  test "memory grows with holes, not with messages" do
    contiguous = UidMap.new(1..2_000)
    assert UidMap.ranges(contiguous) == [{1, 2000}]

    # :erts_debug.size/1 is in words (8 bytes)
    plain_tuple = :erts_debug.size(List.to_tuple(Enum.to_list(1..2_000)))
    assert contiguous.kind == :ranges
    assert :erts_debug.size(contiguous) < 40
    assert plain_tuple > 2_000

    {_, with_holes} = UidMap.remove(contiguous, [10, 500, 1500])
    assert length(UidMap.ranges(with_holes)) == 4
    assert UidMap.size(with_holes) == 1_997
    assert UidMap.uid(with_holes, 10) == 11

    # worst case for runs: every other message deleted -> falls back to a plain tuple,
    # never bigger than one word per message
    sparse = UidMap.new(Enum.take_every(1..4_000, 2))
    assert sparse.kind == :plain
    assert :erts_debug.size(sparse) < plain_tuple + 40
    assert UidMap.seq(sparse, 3999) == 2000 and UidMap.seq(sparse, 4000) == nil
  end
end

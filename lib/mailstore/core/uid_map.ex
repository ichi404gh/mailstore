defmodule Mailstore.Core.UidMap do
  @moduledoc """
  A compact set of UIDs that also numbers them: sequence number N is the N-th UID in
  ascending order.

  Two users:

    * an IMAP session's view of the selected folder. Every session has its own view;
      it changes only where IMAP lets the server report EXISTS / EXPUNGE to it;
    * the account actor's in-memory state of a folder (all UIDs, unseen UIDs), so
      SELECT and SEARCH UNSEEN need no database (`Mailstore.Mailbox.AccountServer`).

  Two representations, whichever is smaller:

    * `:ranges` - runs of consecutive UIDs. UIDs in a folder are mostly consecutive:
      new mail appends at the end, retention cuts a range off the start, only single
      expunges punch holes. A 2 000-message folder without holes is one run. Stored as
      three flat tuples (3 words per run): `firsts`, `lasts`, and `starts` (sequence
      number of each run's first UID), so both lookups are binary searches.
    * `:plain` - one tuple of UIDs (1 word per message), used once there are so many
      holes that runs would take more memory (e.g. every other message deleted).

  So memory is never worse than one word per message and usually a few dozen bytes.
  """
  defstruct kind: :ranges, firsts: {}, lasts: {}, starts: {}, uids: {}, size: 0

  @type t :: %__MODULE__{}

  @doc "From any list or range of UIDs (sorted or not, duplicates allowed)."
  def new(uids), do: uids |> Enum.sort() |> Enum.dedup() |> to_ranges() |> from_ranges()

  @doc "From ascending, non-overlapping `{first, last}` runs (e.g. straight from SQL)."
  def from_ranges(ranges) do
    ranges = merge_adjacent(ranges)
    runs = length(ranges)
    size = Enum.reduce(ranges, 0, fn {a, b}, acc -> acc + b - a + 1 end)

    if runs > 0 and 3 * runs > size do
      uids = Enum.flat_map(ranges, fn {a, b} -> Enum.to_list(a..b) end)
      %__MODULE__{kind: :plain, uids: List.to_tuple(uids), size: size}
    else
      {firsts, lasts} = Enum.unzip(ranges)

      {starts, _} =
        Enum.map_reduce(ranges, 0, fn {a, b}, seen -> {seen + 1, seen + b - a + 1} end)

      %__MODULE__{
        kind: :ranges,
        firsts: List.to_tuple(firsts),
        lasts: List.to_tuple(lasts),
        starts: List.to_tuple(starts),
        size: size
      }
    end
  end

  def size(%__MODULE__{size: size}), do: size

  def ranges(%__MODULE__{kind: :ranges, firsts: f, lasts: l}),
    do: Enum.zip(Tuple.to_list(f), Tuple.to_list(l))

  def ranges(%__MODULE__{kind: :plain, uids: u}), do: u |> Tuple.to_list() |> to_ranges()

  @doc "All UIDs (one entry per message: avoid on hot paths for big folders)."
  def to_list(%__MODULE__{kind: :plain, uids: u}), do: Tuple.to_list(u)
  def to_list(map), do: Enum.flat_map(ranges(map), fn {a, b} -> Enum.to_list(a..b) end)

  def max_uid(%__MODULE__{size: 0}), do: 0
  def max_uid(%__MODULE__{kind: :ranges, lasts: l}), do: elem(l, tuple_size(l) - 1)
  def max_uid(%__MODULE__{kind: :plain, uids: u}), do: elem(u, tuple_size(u) - 1)

  @doc "UID at sequence number `seq` (1-based), or nil."
  def uid(%__MODULE__{size: size}, seq) when seq < 1 or seq > size, do: nil
  def uid(%__MODULE__{kind: :plain, uids: u}, seq), do: elem(u, seq - 1)

  def uid(%__MODULE__{firsts: f, starts: s}, seq) do
    i = last_at_or_below(s, seq, 0, tuple_size(s) - 1)
    elem(f, i) + seq - elem(s, i)
  end

  @doc "Sequence number of `uid`, or nil."
  def seq(%__MODULE__{size: 0}, _uid), do: nil

  def seq(%__MODULE__{kind: :plain, uids: u}, uid) do
    i = last_at_or_below(u, uid, 0, tuple_size(u) - 1)
    if elem(u, i) == uid, do: i + 1, else: nil
  end

  def seq(%__MODULE__{firsts: f, lasts: l, starts: s}, uid) do
    i = last_at_or_below(f, uid, 0, tuple_size(f) - 1)
    if elem(f, i) <= uid and uid <= elem(l, i), do: elem(s, i) + uid - elem(f, i), else: nil
  end

  # Index of the last element <= x in an ascending tuple (0 if all are greater).
  defp last_at_or_below(_t, _x, lo, hi) when lo >= hi, do: lo

  defp last_at_or_below(t, x, lo, hi) do
    mid = div(lo + hi + 1, 2)

    if elem(t, mid) <= x,
      do: last_at_or_below(t, x, mid, hi),
      else: last_at_or_below(t, x, lo, mid - 1)
  end

  # Index of the first element >= x in an ascending tuple (tuple_size if none).
  defp first_at_or_above(_t, _x, lo, hi) when lo >= hi, do: lo

  defp first_at_or_above(t, x, lo, hi) do
    mid = div(lo + hi, 2)

    if elem(t, mid) < x,
      do: first_at_or_above(t, x, mid + 1, hi),
      else: first_at_or_above(t, x, lo, mid)
  end

  @doc "UIDs of the map within `lo..hi`, ascending, without walking the whole folder."
  def uids_between(_map, lo, hi) when lo > hi, do: []

  def uids_between(%__MODULE__{kind: :plain, uids: u}, lo, hi) do
    i = first_at_or_above(u, lo, 0, tuple_size(u))
    take_while_le(u, i, hi, [])
  end

  def uids_between(%__MODULE__{firsts: f, lasts: l}, lo, hi) do
    i = first_at_or_above(l, lo, 0, tuple_size(l))
    collect_runs(f, l, i, lo, hi, [])
  end

  defp take_while_le(u, i, hi, acc) do
    if i < tuple_size(u) and elem(u, i) <= hi,
      do: take_while_le(u, i + 1, hi, [elem(u, i) | acc]),
      else: Enum.reverse(acc)
  end

  defp collect_runs(f, l, i, lo, hi, acc) do
    if i < tuple_size(f) and elem(f, i) <= hi do
      run = Enum.to_list(max(elem(f, i), lo)..min(elem(l, i), hi))
      collect_runs(f, l, i + 1, lo, hi, [run | acc])
    else
      acc |> Enum.reverse() |> List.flatten()
    end
  end

  @doc "Add new UIDs. Already-known ones are ignored, so repeated pushes are harmless."
  def append(map, uids) do
    max = max_uid(map)

    case uids |> Enum.filter(&(&1 > max)) |> Enum.sort() |> Enum.dedup() do
      [] -> map
      new -> from_ranges(ranges(map) ++ to_ranges(new))
    end
  end

  @doc """
  Add UIDs anywhere (a message that lost \\Seen goes back into the unseen set).
  Already-present ones are ignored. For new mail at the end, `append/2` is cheaper.
  """
  def put(map, uids) do
    case uids |> Enum.reject(&seq(map, &1)) |> Enum.sort() |> Enum.dedup() do
      [] -> map
      new -> from_ranges(merge_sorted(ranges(map), to_ranges(new), []))
    end
  end

  # Merge two ascending, non-overlapping run lists; adjacent runs are joined by from_ranges.
  defp merge_sorted([], b, acc), do: Enum.reverse(acc, b)
  defp merge_sorted(a, [], acc), do: Enum.reverse(acc, a)

  defp merge_sorted([{a1, _} = ra | rest_a], [{b1, _} = rb | rest_b], acc) do
    if a1 < b1,
      do: merge_sorted(rest_a, [rb | rest_b], [ra | acc]),
      else: merge_sorted([ra | rest_a], rest_b, [rb | acc])
  end

  @doc """
  Remove UIDs. Returns the sequence numbers they had, in descending order: sending
  EXPUNGE responses in that order keeps every number valid when the client applies them.
  """
  def remove(map, uids) do
    gone = uids |> Enum.filter(&seq(map, &1)) |> Enum.sort() |> Enum.dedup()

    if gone == [] do
      {[], map}
    else
      seqs = gone |> Enum.map(&seq(map, &1)) |> Enum.reverse()
      {seqs, from_ranges(cut(ranges(map), gone, []))}
    end
  end

  # Split runs around the removed UIDs (both lists ascending, every removed UID present).
  defp cut([], _gone, acc), do: Enum.reverse(acc)
  defp cut(ranges, [], acc), do: Enum.reverse(acc, ranges)
  defp cut([{_a, b} = r | rest], [g | _] = gone, acc) when g > b, do: cut(rest, gone, [r | acc])
  defp cut([{a, b} | rest], [g | gone], acc) when g == a and g == b, do: cut(rest, gone, acc)
  defp cut([{a, b} | rest], [g | gone], acc) when g == a, do: cut([{a + 1, b} | rest], gone, acc)
  defp cut([{a, b} | rest], [g | gone], acc) when g == b, do: cut(rest, gone, [{a, b - 1} | acc])

  defp cut([{a, b} | rest], [g | gone], acc),
    do: cut([{g + 1, b} | rest], gone, [{a, g - 1} | acc])

  defp to_ranges([]), do: []

  defp to_ranges([first | rest]) do
    {ranges, current} =
      Enum.reduce(rest, {[], {first, first}}, fn
        uid, {acc, {a, b}} when uid == b + 1 -> {acc, {a, uid}}
        uid, {acc, range} -> {[range | acc], {uid, uid}}
      end)

    Enum.reverse([current | ranges])
  end

  defp merge_adjacent(ranges) do
    ranges
    |> Enum.reduce([], fn
      {a, b}, [{pa, pb} | acc] when a == pb + 1 -> [{pa, b} | acc]
      range, acc -> [range | acc]
    end)
    |> Enum.reverse()
  end
end

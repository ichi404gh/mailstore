defmodule Mailstore.Imap.SequenceSet do
  @moduledoc """
  IMAP sequence sets (`1:3,5,7:*`), for both message sequence numbers and UIDs.
  `*` is the largest number in use. `n:*` with n beyond the largest still matches the
  largest message (RFC 3501), which pollers that ask for `UIDNEXT:*` rely on.
  """
  alias Mailstore.Imap.UidMap

  @type range :: {pos_integer | :star, pos_integer | :star}

  @spec parse(binary) :: {:ok, [range]} | :error
  def parse(bin) do
    bin
    |> String.split(",")
    |> Enum.reduce_while({:ok, []}, fn part, {:ok, acc} ->
      case range(part) do
        {:ok, r} -> {:cont, {:ok, [r | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, ranges} -> {:ok, Enum.reverse(ranges)}
      :error -> :error
    end
  end

  defp range(part) do
    case String.split(part, ":") do
      [a] -> with {:ok, a} <- number(a), do: {:ok, {a, a}}
      [a, b] -> with {:ok, a} <- number(a), {:ok, b} <- number(b), do: {:ok, {a, b}}
      _ -> :error
    end
  end

  defp number("*"), do: {:ok, :star}

  defp number(s) do
    case Integer.parse(s) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> :error
    end
  end

  @doc "Is `n` in the set, given the largest number in use?"
  def member?(ranges, n, max) do
    Enum.any?(ranges, fn {a, b} ->
      {a, b} = {resolve(a, max), resolve(b, max)}
      n >= min(a, b) and n <= max(a, b)
    end)
  end

  @doc "UIDs of the map that the UID set selects, ascending. Cost follows the result size."
  def uids(ranges, map) do
    max = UidMap.max_uid(map)

    ranges
    |> Enum.flat_map(fn {a, b} ->
      {a, b} = {resolve(a, max), resolve(b, max)}
      UidMap.uids_between(map, min(a, b), max(a, b))
    end)
    |> sort_unique(length(ranges))
  end

  @doc "Sequence numbers (1..size) the set selects, ascending."
  def seqs(_ranges, 0), do: []

  def seqs(ranges, size) do
    ranges
    |> Enum.flat_map(fn {a, b} ->
      {a, b} = {resolve(a, size), resolve(b, size)}
      {lo, hi} = {max(min(a, b), 1), min(max(a, b), size)}
      if lo <= hi, do: Enum.to_list(lo..hi), else: []
    end)
    |> sort_unique(length(ranges))
  end

  # One range already yields an ascending list without duplicates.
  defp sort_unique(list, 1), do: list
  defp sort_unique(list, _), do: list |> Enum.sort() |> Enum.dedup()

  @doc "Compact form of a sorted list of numbers, e.g. [1,2,3,5] -> \"1:3,5\"."
  def format(numbers) do
    numbers
    |> Enum.chunk_while(
      nil,
      fn
        n, nil -> {:cont, {n, n}}
        n, {a, b} when n == b + 1 -> {:cont, {a, n}}
        n, acc -> {:cont, acc, {n, n}}
      end,
      fn
        nil -> {:cont, nil}
        acc -> {:cont, acc, nil}
      end
    )
    |> Enum.map_join(",", fn
      {a, a} -> Integer.to_string(a)
      {a, b} -> "#{a}:#{b}"
    end)
  end

  defp resolve(:star, max), do: max
  defp resolve(n, _max), do: n
end

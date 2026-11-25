defmodule Loadgen.Stats do
  @moduledoc """
  Client-side latency per command name, recorded by sessions into a shared ETS table:
  count, sum and a histogram with half-octave buckets (each bucket ~1.41x the previous),
  so percentiles are accurate to about +-20%.
  """
  @table :loadgen_stats

  def init do
    if :ets.whereis(@table) == :undefined,
      do: :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])

    :ok
  end

  def reset, do: :ets.delete_all_objects(@table)

  def record(name, us) do
    :ets.update_counter(@table, {name, :count}, 1, {{name, :count}, 0})
    :ets.update_counter(@table, {name, :sum}, us, {{name, :sum}, 0})
    key = {name, :bucket, bucket(us)}
    :ets.update_counter(@table, key, 1, {key, 0})
  end

  def error(name, reason) do
    key = {:error, name, reason |> inspect() |> String.slice(0, 60)}
    :ets.update_counter(@table, key, 1, {key, 0})
  end

  def total_count do
    :ets.match(@table, {{:_, :count}, :"$1"}) |> List.flatten() |> Enum.sum()
  end

  @doc "[{name, %{count, avg_us, p50_us, p95_us, p99_us}}] sorted by name, and errors."
  def summary do
    rows = :ets.tab2list(@table)
    names = for {{name, :count}, _} <- rows, uniq: true, do: name

    per_name =
      for name <- Enum.sort(names) do
        count = lookup(rows, {name, :count})
        sum = lookup(rows, {name, :sum})

        buckets =
          for({{^name, :bucket, b}, n} <- rows, do: {b, n})
          |> Enum.sort()

        {name,
         %{
           count: count,
           avg_us: div(sum, max(count, 1)),
           p50_us: percentile(buckets, count, 0.50),
           p95_us: percentile(buckets, count, 0.95),
           p99_us: percentile(buckets, count, 0.99)
         }}
      end

    errors = for {{:error, name, reason}, n} <- rows, do: {name, reason, n}
    {per_name, errors}
  end

  defp lookup(rows, key) do
    case List.keyfind(rows, key, 0) do
      {_, v} -> v
      nil -> 0
    end
  end

  defp bucket(us), do: round(2 * :math.log2(max(us, 1)))
  defp upper(b), do: round(:math.pow(2, b / 2))

  defp percentile(buckets, count, p) do
    target = p * count

    Enum.reduce_while(buckets, 0, fn {b, n}, acc ->
      if acc + n >= target, do: {:halt, {:found, upper(b)}}, else: {:cont, acc + n}
    end)
    |> case do
      {:found, us} -> us
      _ -> 0
    end
  end
end

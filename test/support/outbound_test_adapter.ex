defmodule Mailstore.Outbound.TestAdapter do
  @moduledoc """
  Outbound adapter for tests: forwards are sent as `{:outbound, forwards}` to the
  process `listen/0` was called from. `fail_next/2` makes the next calls fail.
  """
  @behaviour Mailstore.Outbound.Adapter

  @impl true
  def children(_config), do: []

  def listen, do: Application.put_env(:mailstore, :outbound_test, %{pid: self(), fail: 0})

  def fail_next(n, reason \\ :leader_not_available),
    do: update(&%{&1 | fail: {n, reason}})

  @impl true
  def deliver(_config, forwards) do
    case Application.get_env(:mailstore, :outbound_test) do
      %{fail: {n, reason}} when n > 0 ->
        update(&%{&1 | fail: {n - 1, reason}})
        {:error, reason}

      %{pid: pid} ->
        send(pid, {:outbound, forwards})
        :ok

      nil ->
        :ok
    end
  end

  defp update(fun) do
    state = Application.fetch_env!(:mailstore, :outbound_test)
    Application.put_env(:mailstore, :outbound_test, fun.(state))
  end
end

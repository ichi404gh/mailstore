defmodule Mailstore.Outbound.Adapter do
  @moduledoc """
  How forwards leave mailstore (`Mailstore.Outbound`). The adapter gets the whole
  outbound config (`config :mailstore, :outbound`), its own options included.
  """

  @doc "Processes the adapter needs (clients, connection pools), started with the context."
  @callback children(config :: keyword) :: [Supervisor.child_spec() | {module, term} | module]

  @doc """
  Take every forward for good, or fail: `{:error, reason}` is retried later; return
  `{:error, %Mailstore.Outbound.Error{transient: false}}` for a forward that can never
  go through as it is.
  """
  @callback deliver(config :: keyword, [Mailstore.Outbound.forward()]) ::
              :ok | {:error, term}
end

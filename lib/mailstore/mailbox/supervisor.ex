defmodule Mailstore.Mailbox.Supervisor do
  @moduledoc "Process tree of the mailbox context: actor registry, actor supervisor, partition upkeep, placement in the cluster."
  use Supervisor

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg) do
    children = [
      {Registry, keys: :unique, name: Mailstore.Mailbox.Registry},
      {DynamicSupervisor, name: Mailstore.Mailbox.AccountSupervisor, strategy: :one_for_one},
      Mailstore.Mailbox.PartitionMaintainer,
      # Last: the node takes accounts only once it can run their actors.
      Mailstore.Mailbox.Placement
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

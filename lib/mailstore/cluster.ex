defmodule Mailstore.Cluster do
  @moduledoc """
  The BEAM cluster: which nodes there are and how they find each other.

  * A `:pg` scope (`:mailstore`) that services join to announce themselves; the
    mailbox context places account actors on its members (`Mailbox.Placement`).
    `:pg` drops the members of a node as soon as the connection to it is lost.
  * `Connector`: connects to the nodes listed in `config :mailstore, :cluster, nodes:`
    (or `MAILSTORE_CLUSTER_NODES`) and keeps retrying, so the cluster heals after a
    restart or a network split. In production the list would come from DNS or the
    orchestrator instead of configuration.

  A node that is not distributed (started without `--name`/`--sname`) is a
  cluster of one; everything works the same.
  """
  use Supervisor

  @scope :mailstore

  def scope, do: @scope

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg) do
    children = [
      %{id: :pg, start: {:pg, :start_link, [@scope]}},
      Mailstore.Cluster.Connector
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Mailstore.Cluster.Connector do
  @moduledoc false
  use GenServer
  require Logger

  @every 5_000

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    send(self(), :connect)
    {:ok, MapSet.new()}
  end

  @impl true
  def handle_info(:connect, connected) do
    connected =
      if Node.alive?() do
        for n <- nodes(), n != node(), reduce: connected do
          acc ->
            up = Node.connect(n) == true
            if up and not MapSet.member?(acc, n), do: Logger.info("cluster: connected to #{n}")
            if up, do: MapSet.put(acc, n), else: MapSet.delete(acc, n)
        end
      else
        connected
      end

    Process.send_after(self(), :connect, @every)
    {:noreply, connected}
  end

  defp nodes, do: Application.get_env(:mailstore, :cluster, []) |> Keyword.get(:nodes, [])
end

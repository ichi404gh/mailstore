defmodule Mailstore.Mailbox.Placement do
  @moduledoc """
  Which node runs the actor of an account.

  Every node with the mailbox context joins the `:pg` group `:mailbox`. The owner of
  an account is chosen among the members by rendezvous hashing (highest
  `hash({account_id, node})` wins): every node computes the same owner from the same
  member list, and when a node joins or leaves only the accounts it wins or held
  move.

  Callers anywhere reach the owner (`AccountServer` routes its calls with `:erpc`),
  so all writes and pushes of an account go through one actor and every session, on
  any node, gets its EXISTS / FLAGS / EXPUNGE.

  When the members change, this node stops the actors of accounts it no longer
  owns. Their sessions see the actor go down, subscribe again (now at the new
  owner) and resync, which is the same path as an actor crash. While nodes briefly
  disagree about the members two actors of one account can exist; that is safe,
  because correctness comes from the database (`Mailbox.Commands`), and it ends as
  soon as the views agree.
  """
  use GenServer
  require Logger

  alias Mailstore.Cluster

  @group :mailbox
  @members {__MODULE__, :members}
  @registry Mailstore.Mailbox.Registry
  @supervisor Mailstore.Mailbox.AccountSupervisor

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc "The node that owns `account_id` under the current members."
  def owner(account_id), do: owner(account_id, members())

  @doc "The owner under a given member list (pure; used by tests and rebalancing)."
  def owner(_account_id, []), do: node()
  def owner(_account_id, [only]), do: only

  def owner(account_id, nodes),
    do: Enum.max_by(nodes, &:erlang.phash2({account_id, &1}, 4_294_967_296))

  @doc "Nodes that run the mailbox context, sorted. Before joining: just this node."
  def members, do: :persistent_term.get(@members, [node()])

  @impl true
  def init(nil) do
    :ok = :pg.join(Cluster.scope(), @group, self())
    {ref, pids} = :pg.monitor(Cluster.scope(), @group)
    publish(pids)
    {:ok, %{ref: ref, pids: MapSet.new(pids)}}
  end

  @impl true
  def handle_info({ref, :join, @group, pids}, %{ref: ref} = st),
    do: changed(st, MapSet.union(st.pids, MapSet.new(pids)))

  def handle_info({ref, :leave, @group, pids}, %{ref: ref} = st),
    do: changed(st, MapSet.difference(st.pids, MapSet.new(pids)))

  def handle_info(_other, st), do: {:noreply, st}

  defp changed(st, pids) do
    before = members()
    now = publish(MapSet.to_list(pids))

    if now != before do
      Logger.info("mailbox placement: members #{inspect(now)}")
      rebalance()
    end

    {:noreply, %{st | pids: pids}}
  end

  defp publish(pids) do
    nodes = pids |> Enum.map(&node/1) |> Enum.uniq() |> Enum.sort()
    nodes = if nodes == [], do: [node()], else: nodes
    if nodes != :persistent_term.get(@members, nil), do: :persistent_term.put(@members, nodes)
    nodes
  end

  # Stop, in the background, the local actors this node no longer owns. Stopping
  # saves their \Recent state (AccountServer.terminate/2).
  defp rebalance do
    me = node()
    nodes = members()

    Task.start(fn ->
      moved =
        for {account_id, pid} <-
              Registry.select(@registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}]),
            owner(account_id, nodes) != me do
          DynamicSupervisor.terminate_child(@supervisor, pid)
        end

      if moved != [], do: Logger.info("mailbox placement: handed over #{length(moved)} accounts")
    end)
  end
end

defmodule Mailstore.Mailbox.AccountServer do
  @moduledoc """
  One process per active account (all of its folders), never one per client.

  * Started lazily by the first caller (an IMAP session selecting a folder, a delivery,
    an APPEND). Concurrent starts are safe: the name is registered atomically, so the
    loser gets `{:error, {:already_started, pid}}` and uses the winner's pid.
  * Lives while it has subscribers, then for `:account_idle_timeout` after the last
    activity, then stops with `:normal`. Nothing to save: the database is the truth.
  * Serializes writes for the account and pushes deltas to the sessions that have the
    affected folder selected. Reads (FETCH, SEARCH) go straight to the shard (`Mailstore.Mailbox` read functions).
  * Keeps the state of the folders sessions open: all UIDs and unseen UIDs, as
    `Mailstore.Core.UidMap` runs (a 2 000-message folder without holes is ~200 bytes).
    Loaded on the first subscribe, kept current by its own writes, dropped on COPY/MOVE
    into the folder and on `reload_all/0`. SELECT and SEARCH UNSEEN read it instead of
    the database.

  Prototype: placement is a local Registry. In a cluster the owner node comes from
  consistent hashing of `account_id` and callers reach it via `:erpc`. Correctness
  does not depend on there being exactly one actor; see `Mailstore.Mailbox.Commands`.

  `subscribe/2` returns the folder view plus a snapshot taken atomically with the
  subscription: `:uids` (a `UidMap`) and `:first_unseen` (UID or nil). Every change
  after it arrives as a push, so the session needs no database query and has no window
  in which a change could be missed.

  Subscriber messages: `{:mailstore, folder_id, event}` where event is one of
  `{:exists, uids, view}`, `{:flags, changes, modseq}`, `{:expunge, uids, modseq}`,
  `{:reload, view}`. A session should monitor the pid returned by `subscribe/2` and
  resubscribe (then resync by MODSEQ) if it goes down.
  """
  use GenServer, restart: :transient
  require Logger

  alias Mailstore.{Directory, Shards}
  alias Mailstore.Core.{Flags, UidMap}
  alias Mailstore.Mailbox.{Commands, Queries}

  @registry Mailstore.Mailbox.Registry
  @supervisor Mailstore.Mailbox.AccountSupervisor
  @call_timeout 15_000

  # ---- placement ----------------------------------------------------------

  @doc "Find or start the actor for an account."
  def ensure(account_id) do
    case Registry.lookup(@registry, account_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(@supervisor, {__MODULE__, account_id}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          :ignore -> {:error, :unknown_account}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  def whereis(account_id) do
    case Registry.lookup(@registry, account_id) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  def start_link(account_id),
    do:
      GenServer.start_link(__MODULE__, account_id,
        name: {:via, Registry, {@registry, account_id}}
      )

  # ---- API ----------------------------------------------------------------

  @doc "Select a folder: returns {:ok, actor_pid, view} and starts pushing deltas to the caller."
  def subscribe(account_id, folder), do: call(account_id, {:subscribe, folder, self()})
  def unsubscribe(account_id), do: call(account_id, {:unsubscribe, self()})

  @doc "SELECT/STATUS data from memory: no database round trip."
  def status(account_id, folder), do: call(account_id, {:status, folder})

  @doc "SEARCH UNSEEN from memory: {:ok, UidMap of unseen UIDs}."
  def unseen(account_id, folder), do: call(account_id, {:unseen, folder})

  @doc "Append `Mailstore.Core.NewMessage`s. Returns {:ok, uids}."
  def append(account_id, folder, msgs, opts \\ []),
    do: call(account_id, {:append, folder, msgs, opts})

  @doc "op: :add | :remove | :replace. Returns {:ok, [%{uid, flags, modseq}]}."
  def store_flags(account_id, folder, uids, op, flags),
    do: call(account_id, {:store, folder, uids, op, flags})

  def expunge(account_id, folder, uids \\ :all), do: call(account_id, {:expunge, folder, uids})

  @doc "Returns {:ok, %{uidvalidity, mapping: [{src_uid, dst_uid}]}} (for COPYUID)."
  def copy(account_id, from, uids, to), do: call(account_id, {:copy, from, uids, to})
  def move(account_id, from, uids, to), do: call(account_id, {:move, from, uids, to})

  @doc "Ask every local actor to reload folder state (after retention jobs)."
  def reload_all do
    for pid <- Registry.select(@registry, [{{:_, :"$1", :_}, [], [:"$1"]}]),
        do: GenServer.cast(pid, :reload)

    :ok
  end

  # The actor may be stopping on idle timeout just as we call it: retry with a fresh one.
  defp call(account_id, msg, attempts \\ 3) do
    case ensure(account_id) do
      {:ok, pid} ->
        try do
          GenServer.call(pid, msg, @call_timeout)
        catch
          :exit, {reason, _} when reason in [:noproc, :normal] and attempts > 1 ->
            call(account_id, msg, attempts - 1)
        end

      {:error, _} = error ->
        error
    end
  end

  # ---- server -------------------------------------------------------------

  @impl true
  def init(account_id) do
    case Directory.get_account(account_id) do
      nil ->
        :ignore

      account ->
        repo = Shards.repo(account.tenant_id)

        state = %{
          account_id: account_id,
          tenant_id: account.tenant_id,
          repo: repo,
          folders: load_folders(repo, account_id),
          cache: %{},
          subs: %{},
          idle_timeout: Application.get_env(:mailstore, :account_idle_timeout, :timer.minutes(10))
        }

        {:ok, state, state.idle_timeout}
    end
  end

  @impl true
  def handle_call({:subscribe, name, pid}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, cached, st} <- cached(st, f) do
      st = drop_sub(st, pid)
      ref = Process.monitor(pid)
      st = put_in(st.subs[pid], {f.id, ref})
      view = Map.merge(f, %{uids: cached.uids, first_unseen: UidMap.uid(cached.unseen, 1)})
      reply({:ok, self(), view}, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:unseen, name}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, cached, st} <- cached(st, f) do
      reply({:ok, cached.unseen}, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:unsubscribe, pid}, _from, st), do: reply(:ok, drop_sub(st, pid))

  def handle_call({:status, name}, _from, st), do: reply(folder(st, name), st)

  def handle_call({:append, name, msgs, opts}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, {uids, counters}} <-
           tx(st, fn -> Commands.append(st.repo, f.id, st.tenant_id, msgs, opts) end) do
      unseen = for {m, uid} <- Enum.zip(msgs, uids), not Flags.seen?(m.flags_mask), do: uid

      st =
        st
        |> put_counters(name, counters)
        |> update_cache(f.id, fn c ->
          %{c | uids: UidMap.append(c.uids, uids), unseen: UidMap.append(c.unseen, unseen)}
        end)

      notify(st, f.id, {:exists, uids, st.folders[norm(name)]})
      reply({:ok, uids}, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:store, name, uids, op, flags}, _from, st) do
    {mask, kws} = Flags.parse(flags)

    with {:ok, f} <- folder(st, name),
         {:ok, {changes, modseq, counters}} <-
           tx(st, fn -> Commands.store_flags(st.repo, f.id, uids, op, mask, kws) end) do
      {seen, unseen} = Enum.split_with(changes, &Flags.seen?(&1.mask))

      st =
        st
        |> put_counters(name, counters)
        |> update_cache(f.id, fn c ->
          {_, without_seen} = UidMap.remove(c.unseen, Enum.map(seen, & &1.uid))
          %{c | unseen: UidMap.put(without_seen, Enum.map(unseen, & &1.uid))}
        end)

      changes =
        Enum.map(
          changes,
          &%{uid: &1.uid, flags: Flags.to_list(&1.mask, &1.keywords), modseq: modseq}
        )

      if changes != [], do: notify(st, f.id, {:flags, changes, modseq})
      reply({:ok, changes}, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:expunge, name, uids}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, {expunged, modseq, counters}} <-
           tx(st, fn -> Commands.expunge(st.repo, f.id, uids) end) do
      st = st |> put_counters(name, counters) |> forget_uids(f.id, expunged)
      if expunged != [], do: notify(st, f.id, {:expunge, expunged, modseq})
      reply({:ok, expunged}, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:copy, from, uids, to}, _from, st) do
    with {:ok, src} <- folder(st, from),
         {:ok, dst} <- folder(st, to),
         {:ok, {mapping, counters}} <-
           tx(st, fn -> Commands.copy(st.repo, src.id, uids, dst.id) end) do
      # Copies keep their source flags, which the actor may not have: reload on next use.
      st = st |> put_counters(to, counters) |> drop_cache(dst.id)

      if mapping != [],
        do: notify(st, dst.id, {:exists, Enum.map(mapping, &elem(&1, 1)), st.folders[norm(to)]})

      reply({:ok, %{uidvalidity: dst.uidvalidity, mapping: mapping}}, st)
    else
      error -> reply(error, st)
    end
  end

  # MOVE = COPY + \Deleted + UID EXPUNGE, in one transaction.
  def handle_call({:move, from, uids, to}, _from, st) do
    with {:ok, src} <- folder(st, from),
         {:ok, dst} <- folder(st, to),
         {:ok, result} <-
           tx(st, fn ->
             {mapping, dst_counters} = Commands.copy(st.repo, src.id, uids, dst.id)
             moved = Enum.map(mapping, &elem(&1, 0))

             {expunged, modseq, src_counters} =
               if moved == [] do
                 {[], nil, nil}
               else
                 Commands.store_flags(st.repo, src.id, moved, :add, Flags.deleted(), [])
                 Commands.expunge(st.repo, src.id, moved)
               end

             {mapping, dst_counters, expunged, modseq, src_counters}
           end) do
      {mapping, dst_counters, expunged, modseq, src_counters} = result

      st =
        st
        |> put_counters(to, dst_counters)
        |> put_counters(from, src_counters)
        |> drop_cache(dst.id)
        |> forget_uids(src.id, expunged)

      if mapping != [] do
        notify(st, dst.id, {:exists, Enum.map(mapping, &elem(&1, 1)), st.folders[norm(to)]})
        notify(st, src.id, {:expunge, expunged, modseq})
      end

      reply({:ok, %{uidvalidity: dst.uidvalidity, mapping: mapping}}, st)
    else
      error -> reply(error, st)
    end
  end

  @impl true
  def handle_cast(:reload, st) do
    st = %{st | folders: load_folders(st.repo, st.account_id), cache: %{}}

    for {pid, {fid, _ref}} <- st.subs do
      case Enum.find(Map.values(st.folders), &(&1.id == fid)) do
        nil -> :ok
        view -> send(pid, {:mailstore, fid, {:reload, view}})
      end
    end

    noreply(st)
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, st) do
    case st.subs do
      %{^pid => {_fid, ^ref}} -> noreply(%{st | subs: Map.delete(st.subs, pid)})
      _ -> noreply(st)
    end
  end

  def handle_info(:timeout, st) do
    if map_size(st.subs) == 0, do: {:stop, :normal, st}, else: noreply(st)
  end

  # ---- helpers ------------------------------------------------------------

  defp tx(st, fun) do
    st.repo.transaction(fun)
  rescue
    e in [Postgrex.Error, DBConnection.ConnectionError] ->
      Logger.error("account #{st.account_id}: #{Exception.message(e)}")
      {:error, e}
  end

  # Folder state for SELECT / SEARCH UNSEEN, loaded once and then kept current.
  defp cached(st, %{id: fid}) do
    case st.cache do
      %{^fid => cached} ->
        {:ok, cached, st}

      _ ->
        cached = %{
          uids: UidMap.from_ranges(st.repo.all(Queries.uid_ranges(fid))),
          unseen: UidMap.from_ranges(st.repo.all(Queries.unseen_uid_ranges(fid)))
        }

        {:ok, cached, put_in(st.cache[fid], cached)}
    end
  rescue
    e in [Postgrex.Error, DBConnection.ConnectionError] ->
      Logger.error("account #{st.account_id}: #{Exception.message(e)}")
      {:error, e}
  end

  defp update_cache(st, folder_id, fun) do
    case st.cache do
      %{^folder_id => cached} -> put_in(st.cache[folder_id], fun.(cached))
      _ -> st
    end
  end

  defp forget_uids(st, _folder_id, []), do: st

  defp forget_uids(st, folder_id, uids) do
    update_cache(st, folder_id, fn c ->
      {_, all} = UidMap.remove(c.uids, uids)
      {_, unseen} = UidMap.remove(c.unseen, uids)
      %{c | uids: all, unseen: unseen}
    end)
  end

  defp drop_cache(st, folder_id), do: %{st | cache: Map.delete(st.cache, folder_id)}

  defp load_folders(repo, account_id) do
    repo.all(Queries.folders_of_account(account_id))
    |> Map.new(fn f ->
      {norm(f.name),
       Map.take(f, [
         :id,
         :name,
         :special_use,
         :uidvalidity,
         :uidnext,
         :highest_modseq,
         :retained_from_uid,
         :messages_count,
         :unseen_count
       ])}
    end)
  end

  # INBOX is case-insensitive in IMAP; other names are case-sensitive.
  defp norm(name), do: if(String.upcase(name) == "INBOX", do: "INBOX", else: name)

  defp folder(st, name) do
    case Map.fetch(st.folders, norm(name)) do
      {:ok, f} -> {:ok, f}
      :error -> {:error, :no_such_folder}
    end
  end

  defp put_counters(st, _name, nil), do: st

  defp put_counters(st, name, counters),
    do: update_in(st.folders[norm(name)], &Map.merge(&1, counters))

  defp notify(st, folder_id, event) do
    for {pid, {^folder_id, _ref}} <- st.subs, do: send(pid, {:mailstore, folder_id, event})
    :ok
  end

  defp drop_sub(st, pid) do
    case Map.pop(st.subs, pid) do
      {{_fid, ref}, subs} ->
        Process.demonitor(ref, [:flush])
        %{st | subs: subs}

      {nil, _} ->
        st
    end
  end

  defp reply(result, st), do: {:reply, result, st, timeout(st)}
  defp noreply(st), do: {:noreply, st, timeout(st)}
  defp timeout(st), do: if(map_size(st.subs) == 0, do: st.idle_timeout, else: :infinity)
end

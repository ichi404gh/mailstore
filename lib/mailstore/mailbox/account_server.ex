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
    `Mailstore.Core.UidMap` runs (a 2 000-message folder without holes is ~200 bytes),
    and the keywords in use (for the FLAGS response).
    Loaded on the first subscribe, kept current by its own writes, dropped on COPY/MOVE
    into the folder and on `reload_all/0`. SELECT and SEARCH UNSEEN read it instead of
    the database.

  Placement: the actor of an account runs on its owner node (`Mailbox.Placement`,
  rendezvous hashing over the cluster members); calls and delivery notifications from
  any node are routed there with `:erpc`, and the actor pushes to sessions on any
  node. Correctness does not depend on there being exactly one actor (during a
  membership change there can briefly be two); see `Mailstore.Mailbox.Commands`.

  Delivery writes behind the actor's back (`Mailstore.Mailbox.deliver/1`, group commit
  across accounts) and then tells it what it stored (`delivered/2`). New UIDs of a
  folder therefore come from several writers, and notifications may arrive out of
  order; sessions must still see each UID once and in ascending order. The actor keeps
  per folder the first UID it has not announced yet (`next_uid`). UIDs of a folder are
  committed in ascending order (the folder row stays locked from UID allocation to
  commit), so when new UIDs start above `next_uid`, the ones in between are already
  committed by a writer whose notification is still on its way: the actor reads them
  from the database and announces them first, and skips them when that notification
  arrives.

  `subscribe/3` returns the folder view plus a snapshot taken atomically with the
  subscription: `:uids` (a `UidMap`), `:first_unseen` (UID or nil), `:keywords` and
  `:recent` (UIDs that are \\Recent for this session).

  \\Recent (RFC 3501 2.3.2): a message is recent in exactly one session, the first that
  selects the folder read-write after it arrived, or the read-write session that has the
  folder selected when it arrives (its push is `{:exists, uids, view, :recent}`). The
  actor tracks the first unclaimed UID per folder in memory, starting from
  `folders.recent_uid`, and writes it back when it stops (one UPDATE per actor
  lifetime, none on SELECT). After a crash some messages are \\Recent again, which is
  harmless: \\Recent is advisory and gone in IMAP4rev2. Every change
  after it arrives as a push, so the session needs no database query and has no window
  in which a change could be missed.

  Subscriber messages: `{:mailstore, folder_id, event}` where event is one of
  `{:exists, uids, view}`, `{:flags, changes, modseq}`, `{:expunge, uids, modseq}`,
  `{:reload, view}`, `{:renamed, view}` (the folder or one above it was renamed: the
  view has the new name), `:deleted` (the folder no longer exists). A session should monitor the pid returned by `subscribe/2` and
  resubscribe (then resync by MODSEQ) if it goes down.
  """
  use GenServer, restart: :transient
  require Logger

  alias Mailstore.{Directory, Shards}
  alias Mailstore.Core.{FolderName, Flags, UidMap}
  alias Mailstore.Mailbox.{Commands, Placement, Queries, Unavailable}

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

  @doc """
  Select a folder: returns {:ok, actor_pid, view} and starts pushing deltas to the caller.
  `read_only: true` (EXAMINE) shows \\Recent messages without claiming them.
  """
  def subscribe(account_id, folder, opts \\ []),
    do: call(account_id, {:subscribe, folder, self(), Keyword.get(opts, :read_only, false)})

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

  @doc """
  CREATE, with any missing parents. Errors: `:invalid_name`, `:already_exists`.
  """
  def create_folder(account_id, name), do: call(account_id, {:create_folder, name})

  @doc """
  DELETE with all messages. Errors: `:no_such_folder`, `:inbox`, `:has_children`
  (delete the children first, as Dovecot requires). Sessions that have it selected get
  `:deleted`.
  """
  def delete_folder(account_id, name), do: call(account_id, {:delete_folder, name})

  @doc """
  RENAME, children included; UIDs and UIDVALIDITY stay. Errors: `:no_such_folder`,
  `:already_exists`, `:inbox` (renaming INBOX is not supported), `:invalid_name`,
  `:into_itself`. Sessions on a renamed folder get `{:renamed, view}`.
  """
  def rename_folder(account_id, old, new), do: call(account_id, {:rename_folder, old, new})

  @doc "SUBSCRIBE / UNSUBSCRIBE an existing folder. Error: `:no_such_folder`."
  def set_subscribed(account_id, name, on?), do: call(account_id, {:set_subscribed, name, on?})

  @doc "Returns {:ok, %{uidvalidity, mapping: [{src_uid, dst_uid}]}} (for COPYUID)."
  def copy(account_id, from, uids, to), do: call(account_id, {:copy, from, uids, to})
  def move(account_id, from, uids, to), do: call(account_id, {:move, from, uids, to})

  @doc """
  Messages stored by a group commit (`Mailstore.Mailbox.deliver/1`). Only a running actor
  needs to hear about it: one that starts later reads the folders after the commit.
  `update`: `%{folder_id, uids, unseen, keywords, counters}`.
  """
  def delivered(account_id, update) do
    owner = Placement.owner(account_id)
    if owner != node(), do: :erpc.cast(owner, __MODULE__, :delivered_local, [account_id, update])
    # Also a local actor, if any: the owner is here, or an old one is still handing over.
    delivered_local(account_id, update)
  end

  @doc false
  def delivered_local(account_id, update) do
    case whereis(account_id) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:delivered, update})
    end
  end

  @doc false
  # A tenant moved to another shard: its actors here hold the old shard's repo.
  def stop_local(account_ids) do
    for id <- account_ids,
        pid = whereis(id),
        pid != nil,
        do: DynamicSupervisor.terminate_child(@supervisor, pid)

    :ok
  end

  @doc "Ask every actor in the cluster to reload folder state (after retention jobs)."
  def reload_all do
    for n <- Placement.members(), n != node(), do: :erpc.cast(n, __MODULE__, :reload_local, [])
    reload_local()
  end

  @doc """
  Reload the actors of these accounts wherever they run, e.g. after retention changed
  their folders: their sessions resync and see EXPUNGE for what went.
  """
  def reload(account_ids) do
    for n <- Placement.members(),
        n != node(),
        do: :erpc.cast(n, __MODULE__, :reload_local, [account_ids])

    reload_local(account_ids)
  end

  @doc false
  def reload_local(account_ids) when is_list(account_ids) do
    for id <- account_ids, pid = whereis(id), do: GenServer.cast(pid, :reload)
    :ok
  end

  @doc false
  def reload_local do
    for pid <- Registry.select(@registry, [{{:_, :"$1", :_}, [], [:"$1"]}]),
        do: GenServer.cast(pid, :reload)

    :ok
  end

  # To the owner node. If it has just gone away, wait for the members to update and
  # ask the new owner. (A write that reached a node which then died may be repeated:
  # a duplicate is the lesser evil than a lost APPEND.)
  defp call(account_id, msg, attempts \\ 5, moved \\ 30) do
    owner = Placement.owner(account_id)

    result =
      if owner == node() do
        local_call(account_id, msg)
      else
        try do
          :erpc.call(owner, __MODULE__, :local_call, [account_id, msg], @call_timeout + 1_000)
        catch
          :error, {:erpc, :noconnection} when attempts > 1 ->
            Process.sleep(200)
            {:retry, :noconnection}
        end
      end

    case result do
      {:retry, :noconnection} ->
        call(account_id, msg, attempts - 1, moved)

      # The tenant moved to another shard: until the directory switch is visible the
      # write keeps hitting the fenced old shard, so wait a little between tries.
      {:error, :tenant_moved} when moved > 1 ->
        Process.sleep(100)
        call(account_id, msg, attempts, moved - 1)

      {:error, :unavailable} ->
        raise Unavailable

      other ->
        other
    end
  end

  @doc false
  # Runs on the owner node. The actor may be stopping (idle timeout, or handed over
  # to another node) just as we call it: retry with a fresh one.
  def local_call(account_id, msg, attempts \\ 3) do
    case ensure(account_id) do
      {:ok, pid} ->
        try do
          with {:error, :tenant_moved} = moved <- GenServer.call(pid, msg, @call_timeout) do
            # Look the tenant's shard up again here, where the next actor will start.
            Mailstore.Directory.Cache.refresh()
            moved
          end
        catch
          :exit, {reason, _} when attempts > 1 ->
            if stopping?(reason),
              do: local_call(account_id, msg, attempts - 1),
              else: exit({reason, {GenServer, :call, [pid, msg, @call_timeout]}})
        end

      {:error, _} = error ->
        error
    end
  end

  defp stopping?(reason),
    do: reason in [:noproc, :normal, :shutdown] or match?({:shutdown, _}, reason)

  # ---- server -------------------------------------------------------------

  @impl true
  def init(account_id) do
    case Directory.get_account(account_id) do
      nil ->
        :ignore

      account ->
        # So that terminate/2 runs on supervisor shutdown too and saves \\Recent.
        Process.flag(:trap_exit, true)
        repo = Shards.repo(account.tenant_id)

        state =
          track_recent(%{
            account_id: account_id,
            tenant_id: account.tenant_id,
            repo: repo,
            folders: load_folders(repo, account_id),
            recent_from: %{},
            next_uid: %{},
            cache: %{},
            subs: %{},
            idle_timeout:
              Application.get_env(:mailstore, :account_idle_timeout, :timer.minutes(10))
          })

        {:ok, state, state.idle_timeout}
    end
  end

  @impl true
  def handle_call({:subscribe, name, pid, read_only}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, cached, st} <- cached(st, f) do
      st = drop_sub(st, pid)
      ref = Process.monitor(pid)
      st = put_in(st.subs[pid], {f.id, ref, not read_only})
      recent = UidMap.uids_between(cached.uids, recent_from(st, f), UidMap.max_uid(cached.uids))
      st = if read_only, do: st, else: put_in(st.recent_from[f.id], f.uidnext)

      view =
        Map.merge(f, %{
          uids: cached.uids,
          first_unseen: UidMap.uid(cached.unseen, 1),
          keywords: cached.keywords |> MapSet.to_list() |> Enum.sort(),
          recent: recent
        })

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

  def handle_call({:status, name}, _from, st) do
    case folder(st, name) do
      {:ok, f} -> reply({:ok, Map.put(f, :recent, recent_count(st, f))}, st)
      error -> reply(error, st)
    end
  end

  def handle_call({:append, name, msgs, opts}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, {uids, counters}} <-
           tx(st, fn -> Commands.append(st.repo, f.id, st.tenant_id, msgs, opts) end) do
      unseen = for {m, uid} <- Enum.zip(msgs, uids), not Flags.seen?(m.flags_mask), do: uid
      keywords = Enum.flat_map(msgs, & &1.keywords)
      st = advance(st, norm(name), uids, unseen, keywords, counters)
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
          keywords = MapSet.new(Enum.flat_map(changes, & &1.keywords))

          %{
            c
            | unseen: UidMap.put(without_seen, Enum.map(unseen, & &1.uid)),
              keywords: MapSet.union(c.keywords, keywords)
          }
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
      st = drop_cache(st, dst.id)
      st = advance(st, norm(to), Enum.map(mapping, &elem(&1, 1)), [], [], counters)

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
        |> put_counters(from, src_counters)
        |> drop_cache(dst.id)
        |> forget_uids(src.id, expunged)

      if mapping != [], do: notify(st, src.id, {:expunge, expunged, modseq})
      st = advance(st, norm(to), Enum.map(mapping, &elem(&1, 1)), [], [], dst_counters)

      reply({:ok, %{uidvalidity: dst.uidvalidity, mapping: mapping}}, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:create_folder, name}, _from, st) do
    with {:ok, name} <- FolderName.normalize(name),
         :error <- Map.fetch(st.folders, name),
         missing = Enum.reject(FolderName.parents(name), &Map.has_key?(st.folders, &1)),
         {:ok, :ok} <-
           tx(st, fn ->
             Commands.create_folders(st.repo, st.tenant_id, st.account_id, missing ++ [name])
           end) do
      reply(:ok, track_recent(%{st | folders: load_folders(st.repo, st.account_id)}))
    else
      {:ok, _existing} -> reply({:error, :already_exists}, st)
      error -> reply(error, st)
    end
  end

  def handle_call({:set_subscribed, name, on?}, _from, st) do
    with {:ok, f} <- folder(st, name),
         {:ok, :ok} <- tx(st, fn -> Commands.set_subscribed(st.repo, f.id, on?) end) do
      reply(:ok, st)
    else
      error -> reply(error, st)
    end
  end

  def handle_call({:delete_folder, name}, _from, st) do
    with {:ok, name} <- FolderName.normalize(name),
         {:ok, f} <- folder(st, name),
         :ok <- if(name == "INBOX", do: {:error, :inbox}, else: :ok),
         :ok <- if(has_children?(st, name), do: {:error, :has_children}, else: :ok),
         {:ok, :ok} <- tx(st, fn -> Commands.delete_folder(st.repo, f.id) end) do
      notify(st, f.id, :deleted)
      st = drop_cache(%{st | folders: load_folders(st.repo, st.account_id)}, f.id)
      reply(:ok, st)
    else
      {:error, :invalid_name} -> reply({:error, :no_such_folder}, st)
      error -> reply(error, st)
    end
  end

  def handle_call({:rename_folder, old, new}, _from, st) do
    with {:ok, old} <- FolderName.normalize(old),
         {:ok, _} <- folder(st, old),
         {:ok, new} <- FolderName.normalize(new),
         :ok <- if(old == "INBOX", do: {:error, :inbox}, else: :ok),
         :ok <- if(Map.has_key?(st.folders, new), do: {:error, :already_exists}, else: :ok),
         :ok <- if(FolderName.within?(new, old), do: {:error, :into_itself}, else: :ok),
         missing = Enum.reject(FolderName.parents(new), &Map.has_key?(st.folders, &1)),
         {:ok, renamed} <-
           tx(st, fn ->
             if missing != [],
               do: Commands.create_folders(st.repo, st.tenant_id, st.account_id, missing)

             Commands.rename_folder(st.repo, st.account_id, old, new)
           end) do
      st = track_recent(%{st | folders: load_folders(st.repo, st.account_id)})

      for {id, name} <- renamed, do: notify(st, id, {:renamed, st.folders[name]})
      reply(:ok, st)
    else
      error -> reply(error, st)
    end
  end

  @impl true
  def handle_cast({:delivered, update}, st) do
    case Enum.find(st.folders, fn {_name, f} -> f.id == update.folder_id end) do
      nil ->
        noreply(st)

      {name, _f} ->
        noreply(advance(st, name, update.uids, update.unseen, update.keywords, update.counters))
    end
  end

  # Sessions resync from the database after {:reload, view}: start counting from there.
  def handle_cast(:reload, st) do
    folders = load_folders(st.repo, st.account_id)
    next_uid = Map.new(Map.values(folders), &{&1.id, &1.uidnext})
    st = %{st | folders: folders, cache: %{}, next_uid: next_uid}

    for {pid, {fid, _ref, _rw}} <- st.subs do
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
      %{^pid => {_fid, ^ref, _rw}} -> noreply(%{st | subs: Map.delete(st.subs, pid)})
      _ -> noreply(st)
    end
  end

  def handle_info(:timeout, st) do
    if map_size(st.subs) == 0, do: {:stop, :normal, st}, else: noreply(st)
  end

  def handle_info({:EXIT, _pid, _reason}, st), do: noreply(st)
  def handle_info(:tenant_moved, st), do: {:stop, {:shutdown, :tenant_moved}, st}

  @impl true
  def terminate(_reason, st) do
    changed =
      for {_name, f} <- st.folders,
          from = Map.get(st.recent_from, f.id),
          from != nil and from > f.recent_uid,
          do: {f.id, from}

    if changed != [] do
      {ids, froms} = Enum.unzip(changed)

      st.repo.query!(
        """
        UPDATE folders f SET recent_uid = v.r
        FROM unnest($1::bigint[], $2::bigint[]) AS v(id, r)
        WHERE f.id = v.id AND f.recent_uid < v.r
        """,
        [ids, froms]
      )
    end

    :ok
  rescue
    e in [Postgrex.Error, DBConnection.ConnectionError] ->
      Logger.warning("account #{st.account_id}: \\Recent not saved: #{Exception.message(e)}")
      :ok
  end

  # ---- helpers ------------------------------------------------------------

  defp tx(st, fun) do
    st.repo.transaction(fun)
  rescue
    Mailstore.Shards.TenantMoved ->
      # This actor's repo is the old shard: stop after replying; the caller retries
      # and the next actor starts on the tenant's new shard.
      send(self(), :tenant_moved)
      {:error, :tenant_moved}

    e in [Postgrex.Error, DBConnection.ConnectionError] ->
      db_error(st, e)
  end

  # A failover or a lost connection: the caller gets `Mailstore.Mailbox.Unavailable`
  # and may retry. Anything else is a bug and is passed on as is.
  defp db_error(st, e) do
    if Unavailable.transient?(e) do
      Logger.warning("account #{st.account_id}: shard unavailable: #{Exception.message(e)}")
      {:error, :unavailable}
    else
      Logger.error("account #{st.account_id}: #{Exception.message(e)}")
      {:error, e}
    end
  end

  # Folder state for SELECT / SEARCH UNSEEN, loaded once and then kept current.
  defp cached(st, %{id: fid} = f) do
    case st.cache do
      %{^fid => cached} ->
        {:ok, cached, st}

      _ ->
        cached = %{
          uids: UidMap.from_ranges(st.repo.all(Queries.uid_ranges(fid))),
          unseen: UidMap.from_ranges(st.repo.all(Queries.unseen_uid_ranges(fid))),
          keywords: MapSet.new(st.repo.all(Queries.folder_keywords(fid)))
        }

        # The load may see messages whose notification is still on its way. Sessions
        # already subscribed (the cache was dropped under them by a COPY) get them now;
        # the notification will then be skipped.
        from = next_uid(st, f)
        missed = UidMap.uids_between(cached.uids, from, UidMap.max_uid(cached.uids))
        st = put_in(st.cache[fid], cached)
        st = if missed == [], do: st, else: announce(st, f, missed)
        {:ok, cached, st}
    end
  rescue
    e in [Postgrex.Error, DBConnection.ConnectionError] -> db_error(st, e)
  end

  # New UIDs in folder `name` from any writer (see the moduledoc): skip those already
  # announced, read and announce first any committed ones below them, then these.
  defp advance(st, name, uids, unseen, keywords, counters) do
    st = merge_counters(st, name, counters)
    f = st.folders[name]
    from = next_uid(st, f)

    case Enum.filter(uids, &(&1 >= from)) do
      [] ->
        st

      [first | _] = fresh ->
        {gap, gap_unseen, gap_keywords} =
          if first > from, do: read_new(st, f.id, from, first), else: {[], [], []}

        uids = gap ++ fresh
        unseen = gap_unseen ++ Enum.filter(unseen, &(&1 >= from))

        st
        |> update_cache(f.id, fn c ->
          %{
            c
            | uids: UidMap.append(c.uids, uids),
              unseen: UidMap.append(c.unseen, unseen),
              keywords: MapSet.union(c.keywords, MapSet.new(gap_keywords ++ keywords))
          }
        end)
        |> announce(f, uids)
    end
  end

  # Committed messages of a folder with from <= uid < to: {uids, unseen uids, keywords}.
  defp read_new(st, folder_id, from, to) do
    %{rows: rows} =
      st.repo.query!(
        """
        SELECT uid, flags, keywords FROM messages
        WHERE folder_id = $1 AND uid >= $2 AND uid < $3 ORDER BY uid
        """,
        [folder_id, from, to]
      )

    {Enum.map(rows, &hd/1), for([uid, flags, _] <- rows, not Flags.seen?(flags), do: uid),
     Enum.flat_map(rows, &List.last/1)}
  end

  defp announce(st, f, uids) do
    next = List.last(uids) + 1
    view = %{f | uidnext: max(f.uidnext, next)}
    st = put_in(st.next_uid[f.id], next)
    notify_exists(st, f.id, uids, view)
  end

  defp next_uid(st, f), do: Map.get(st.next_uid, f.id, f.uidnext)

  # Counters only move forward: a notification can be older than what the actor has.
  defp merge_counters(st, _name, nil), do: st

  defp merge_counters(st, name, counters) do
    if counters.highest_modseq >= st.folders[name].highest_modseq,
      do: put_counters(st, name, counters),
      else: st
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
         :recent_uid,
         :highest_modseq,
         :retained_from_uid,
         :messages_count,
         :unseen_count
       ])}
    end)
  end

  # INBOX is case-insensitive in IMAP (also as a prefix); other names are case-sensitive.
  defp norm(name) do
    case FolderName.normalize(name) do
      {:ok, name} -> name
      {:error, _} -> name
    end
  end

  defp has_children?(st, name),
    do: Enum.any?(Map.keys(st.folders), &(&1 != name and FolderName.within?(&1, name)))

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
    for {pid, {^folder_id, _ref, _rw}} <- st.subs, do: send(pid, {:mailstore, folder_id, event})
    :ok
  end

  # New messages: one read-write session with the folder selected gets them as \\Recent;
  # without one they stay unclaimed for the next SELECT.
  defp notify_exists(st, folder_id, uids, view) do
    claimer =
      Enum.find_value(st.subs, fn {pid, {fid, _ref, rw}} -> (fid == folder_id and rw) && pid end)

    for {pid, {^folder_id, _ref, _rw}} <- st.subs do
      event = if pid == claimer, do: {:exists, uids, view, :recent}, else: {:exists, uids, view}
      send(pid, {:mailstore, folder_id, event})
    end

    if claimer, do: put_in(st.recent_from[folder_id], List.last(uids) + 1), else: st
  end

  defp recent_from(st, f), do: Map.get(st.recent_from, f.id, f.uidnext)

  # Where each folder's \\Recent and announced UIDs start when the actor first sees it.
  defp track_recent(st) do
    put_new = fn map, field ->
      Enum.reduce(Map.values(st.folders), map, &Map.put_new(&2, &1.id, Map.fetch!(&1, field)))
    end

    %{
      st
      | recent_from: put_new.(st.recent_from, :recent_uid),
        next_uid: put_new.(st.next_uid, :uidnext)
    }
  end

  # STATUS RECENT: unclaimed messages. Without the folder state in memory, everything
  # appended since the actor started (expunges need a selected session, which loads it).
  defp recent_count(st, f), do: recent_in(Map.get(st.cache, f.id), recent_from(st, f), f)

  defp recent_in(nil, from, f), do: max(f.uidnext - from, 0)

  defp recent_in(c, from, _f),
    do: length(UidMap.uids_between(c.uids, from, UidMap.max_uid(c.uids)))

  defp drop_sub(st, pid) do
    case Map.pop(st.subs, pid) do
      {{_fid, ref, _rw}, subs} ->
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

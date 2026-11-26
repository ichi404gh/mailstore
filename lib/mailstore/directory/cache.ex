defmodule Mailstore.Directory.Cache do
  @moduledoc """
  The directory in ETS on every node: recipient lookup at delivery and LOGIN cost
  microseconds and no database round trip. Measured with ~860k accounts: full reload
  ~5 s, lookup ~10 us (see bench/README.md for memory).

  Tables (public, read-concurrent; this process owns and refreshes them):
    * accounts:    id -> %{id, tenant_id, domain_id, address, password_hash, status, kind}
    * addresses:   address -> id
    * forwardings: {source_account_id, forwarding_id} -> forwarding map (ordered_set,
      so all rules of one account are a prefix range)
    * auth:        {account_id, fingerprint} -> expires_at, successful LOGINs (see below)
    * tenants:     id -> shard id (where the tenant's mail lives, `Mailstore.Shards`)
    * rules:       {scope, rule_id} -> compiled delivery rule (`Mailstore.Core.Rules`),
      scope `:global`, `{:tenant, id}` or `{:account, id}`; the global rules are also
      kept as one list in a persistent term, so delivery does not copy them per message

  Freshness: loaded at start (until then callers fall back to the database), then every
  `poll_ms` it reads rows whose `updated_at` moved, and every `full_reload_ms` it reloads
  everything to drop hard-deleted rows. Writes made through `Mailstore.Directory` on this
  node are applied immediately. In production the change feed would come from the
  control plane (Kafka) instead of polling.

  Login cache: PBKDF2 with a realistic cost takes tens of milliseconds, and polling clients
  log in again every few minutes. A successful check is remembered as
  `HMAC(node_secret, password_hash <> password)` for `auth_ttl_ms`. A password change
  changes `password_hash`, so old entries stop matching. Failures are never cached.
  """
  use GenServer
  require Logger

  import Ecto.Query
  alias Mailstore.Core.Rules
  alias Mailstore.Directory.Schemas.{Account, DeliveryRule, Forwarding, Tenant}
  alias Mailstore.Repo

  # How long after stamping its rows a directory transaction may still commit.
  @late_commit_s 5

  @accounts :mailstore_dir_accounts
  @addresses :mailstore_dir_addresses
  @forwardings :mailstore_dir_forwardings
  @auth :mailstore_dir_auth
  @rules :mailstore_dir_rules
  @tenants :mailstore_dir_tenants
  @global_rules {__MODULE__, :global_rules}
  @loaded {__MODULE__, :loaded}
  @secret {__MODULE__, :secret}
  @batch 5_000

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  ## Reads (any process)

  @doc "True once the initial load finished; until then callers use the database."
  def loaded?, do: :persistent_term.get(@loaded, false)

  def account(id) do
    case :ets.lookup(@accounts, id) do
      [{_, account}] -> account
      [] -> nil
    end
  end

  def lookup(address) do
    case :ets.lookup(@addresses, String.downcase(address)) do
      [{_, id}] -> account(id)
      [] -> nil
    end
  end

  def forwardings(account_id) do
    :ets.select(@forwardings, [{{{account_id, :_}, :"$1"}, [], [:"$1"]}])
  end

  def tenant_shard(tenant_id) do
    case :ets.lookup(@tenants, tenant_id) do
      [{_, shard}] -> shard
      [] -> nil
    end
  end

  @doc "Compiled delivery rules for a mailbox: global, its tenant's and its own (unsorted)."
  def rules(tenant_id, account_id) do
    :persistent_term.get(@global_rules, []) ++
      :ets.select(@rules, [{{{{:tenant, tenant_id}, :_}, :"$1"}, [], [:"$1"]}]) ++
      :ets.select(@rules, [{{{{:account, account_id}, :_}, :"$1"}, [], [:"$1"]}])
  end

  def auth_ok?(account, password) do
    case :ets.lookup(@auth, {account.id, fingerprint(account, password)}) do
      [{_, expires}] -> expires > now()
      [] -> false
    end
  end

  def remember_auth(account, password) do
    ttl = config(:auth_ttl_ms, :timer.hours(1))
    :ets.insert(@auth, {{account.id, fingerprint(account, password)}, now() + ttl})
    :ok
  end

  ## Writes (Directory on this node)

  def put_account(%Account{} = account),
    do: GenServer.call(__MODULE__, {:put_account, compact(account)})

  def put_forwarding(%Forwarding{} = fwd),
    do: GenServer.call(__MODULE__, {:put_forwarding, compact(fwd)})

  def put_tenant(%Tenant{} = tenant),
    do: GenServer.call(__MODULE__, {:put_tenant, compact(tenant)})

  def put_rule(%DeliveryRule{} = rule), do: GenServer.call(__MODULE__, {:put_rule, compact(rule)})
  def delete_rule(%DeliveryRule{} = rule), do: GenServer.call(__MODULE__, {:delete_rule, rule})

  @doc "Reload all rules now (after a bulk import that deleted rows)."
  def reload_rules, do: GenServer.call(__MODULE__, :reload_rules, 60_000)

  @doc "Pick up database changes now (instead of waiting for the next poll)."
  def refresh, do: GenServer.call(__MODULE__, :refresh, 60_000)

  @doc "Empty everything (tests truncate the database)."
  def reset, do: GenServer.call(__MODULE__, :reset)

  ## Server

  @impl true
  def init(nil) do
    opts = [:public, :named_table, read_concurrency: true]
    :ets.new(@accounts, [:set | opts])
    :ets.new(@addresses, [:set | opts])
    :ets.new(@forwardings, [:ordered_set | opts])
    :ets.new(@auth, [:set, {:write_concurrency, true} | opts])
    :ets.new(@rules, [:ordered_set | opts])
    :ets.new(@tenants, [:set | opts])
    :persistent_term.put(@global_rules, [])
    :persistent_term.put(@secret, :crypto.strong_rand_bytes(32))
    :persistent_term.put(@loaded, false)

    {:ok, %{accounts_since: nil, forwardings_since: nil, rules_since: nil, tenants_since: nil},
     {:continue, :load}}
  end

  @impl true
  def handle_continue(:load, st) do
    st = full_reload(st)
    :persistent_term.put(@loaded, true)
    schedule(:poll, config(:poll_ms, 1_000))
    schedule(:full_reload, config(:full_reload_ms, :timer.minutes(10)))
    schedule(:sweep_auth, :timer.minutes(5))
    {:noreply, st}
  rescue
    e ->
      Logger.warning("directory cache: initial load failed (#{Exception.message(e)}), retrying")
      Process.send_after(self(), :retry_load, 5_000)
      {:noreply, st}
  end

  @impl true
  def handle_call({:put_account, account}, _from, st), do: {:reply, upsert_account(account), st}

  def handle_call({:put_forwarding, fwd}, _from, st) do
    :ets.insert(@forwardings, {{fwd.source_account_id, fwd.id}, fwd})
    {:reply, :ok, st}
  end

  def handle_call({:put_tenant, tenant}, _from, st) do
    upsert_tenant(tenant)
    {:reply, :ok, st}
  end

  def handle_call({:put_rule, rule}, _from, st) do
    upsert_rule(rule)
    publish_global()
    {:reply, :ok, st}
  end

  def handle_call({:delete_rule, rule}, _from, st) do
    :ets.delete(@rules, {DeliveryRule.scope_key(rule), rule.id})
    publish_global()
    {:reply, :ok, st}
  end

  def handle_call(:reload_rules, _from, st), do: {:reply, :ok, reload_rules(st)}

  def handle_call(:refresh, _from, st), do: {:reply, :ok, poll(st)}

  def handle_call(:reset, _from, st) do
    Enum.each(
      [@accounts, @addresses, @forwardings, @auth, @rules, @tenants],
      &:ets.delete_all_objects/1
    )

    :persistent_term.put(@global_rules, [])

    {:reply, :ok,
     %{st | accounts_since: nil, forwardings_since: nil, rules_since: nil, tenants_since: nil}}
  end

  @impl true
  def handle_info(:retry_load, st), do: handle_continue(:load, st)

  def handle_info(:poll, st) do
    st = safely(st, &poll/1)
    schedule(:poll, config(:poll_ms, 1_000))
    {:noreply, st}
  end

  def handle_info(:full_reload, st) do
    st = safely(st, &full_reload/1)
    schedule(:full_reload, config(:full_reload_ms, :timer.minutes(10)))
    {:noreply, st}
  end

  def handle_info(:sweep_auth, st) do
    now = now()
    :ets.select_delete(@auth, [{{:_, :"$1"}, [{:<, :"$1", now}], [true]}])
    schedule(:sweep_auth, :timer.minutes(5))
    {:noreply, st}
  end

  ## Loading

  # Everything, then drop what no longer exists (hard deletes).
  defp full_reload(st) do
    {account_ids, accounts_since} = load_all(Account, &upsert_account/1)
    {fwd_keys, forwardings_since} = load_all(Forwarding, &upsert_forwarding/1)

    drop_missing(@accounts, account_ids, fn id ->
      case account(id) do
        %{address: address} -> :ets.delete(@addresses, address)
        nil -> :ok
      end
    end)

    drop_missing(@forwardings, fwd_keys, fn _ -> :ok end)
    {tenant_ids, tenants_since} = load_all(Tenant, &upsert_tenant/1)
    drop_missing(@tenants, tenant_ids, fn _ -> :ok end)
    st = reload_rules(%{st | tenants_since: tenants_since})
    %{st | accounts_since: accounts_since, forwardings_since: forwardings_since}
  end

  defp reload_rules(st) do
    {rule_keys, rules_since} = load_all(DeliveryRule, &upsert_rule/1)
    drop_missing(@rules, rule_keys, fn _ -> :ok end)
    publish_global()
    %{st | rules_since: rules_since}
  end

  defp load_all(schema, upsert) do
    {:ok, result} =
      Repo.transaction(
        fn ->
          from(r in schema, order_by: r.id)
          |> Repo.stream(max_rows: @batch)
          |> Enum.reduce({MapSet.new(), nil}, fn row, {keys, since} ->
            since = max_time(since, row.updated_at)
            row = compact(row)
            upsert.(row)
            {MapSet.put(keys, key(row)), since}
          end)
        end,
        timeout: :infinity
      )

    result
  end

  defp poll(st) do
    {accounts, accounts_since} = changed_since(Account, st.accounts_since)
    Enum.each(accounts, &upsert_account/1)
    {fwds, forwardings_since} = changed_since(Forwarding, st.forwardings_since)
    Enum.each(fwds, &upsert_forwarding/1)
    {tenants, tenants_since} = changed_since(Tenant, st.tenants_since)
    Enum.each(tenants, &upsert_tenant/1)
    {rules, rules_since} = changed_since(DeliveryRule, st.rules_since)
    Enum.each(rules, &upsert_rule/1)
    if Enum.any?(rules, &match?({:global, _}, &1.key)), do: publish_global()

    %{
      st
      | accounts_since: accounts_since,
        forwardings_since: forwardings_since,
        rules_since: rules_since,
        tenants_since: tenants_since
    }
  end

  # Rows stamped after the last one seen, and, while they are recent, also those at the
  # same or a slightly earlier stamp: a transaction commits after it stamps its rows, so
  # rows can appear behind the boundary for a moment (re-applying them is harmless).
  # Only recent ones, or a bulk change that stamped many rows alike would be read again
  # on every poll until a newer row comes along.
  defp changed_since(schema, since) do
    query = from(r in schema, order_by: r.updated_at)

    query =
      if since,
        do:
          from(r in query,
            where: r.updated_at > ^since or r.updated_at >= ago(@late_commit_s, "second")
          ),
        else: query

    rows = Repo.all(query)
    {Enum.map(rows, &compact/1), Enum.reduce(rows, since, &max_time(&2, &1.updated_at))}
  end

  defp upsert_account(account) do
    case account(account.id) do
      %{address: old} when old != account.address -> :ets.delete(@addresses, old)
      _ -> :ok
    end

    :ets.insert(@accounts, {account.id, account})
    :ets.insert(@addresses, {account.address, account.id})
    :ok
  end

  defp upsert_forwarding(fwd),
    do: :ets.insert(@forwardings, {{fwd.source_account_id, fwd.id}, fwd})

  defp upsert_tenant(%{id: id, shard: shard}), do: :ets.insert(@tenants, {id, shard})

  # A disabled or invalid rule leaves the cache.
  defp upsert_rule(%{key: key, rule: nil}), do: :ets.delete(@rules, key)
  defp upsert_rule(%{key: key, rule: rule}), do: :ets.insert(@rules, {key, rule})

  defp publish_global do
    global = :ets.select(@rules, [{{{:global, :_}, :"$1"}, [], [:"$1"]}])

    if global != :persistent_term.get(@global_rules, []),
      do: :persistent_term.put(@global_rules, global)
  end

  defp drop_missing(table, keep, on_drop) do
    :ets.foldl(
      fn {key, _}, :ok ->
        unless MapSet.member?(keep, key), do: drop(table, key, on_drop)
        :ok
      end,
      :ok,
      table
    )
  end

  defp drop(table, key, on_drop) do
    on_drop.(key)
    :ets.delete(table, key)
  end

  defp key(%{key: key}), do: key
  defp key(%{source_account_id: source, id: id}), do: {source, id}
  defp key(%{id: id}), do: id

  # Only what lookups need: no Ecto metadata, associations or timestamps (a DateTime is
  # ~10 fields and almost doubled the per-account size).
  defp compact(%Account{} = a),
    do: Map.take(a, [:id, :tenant_id, :domain_id, :address, :password_hash, :status, :kind])

  defp compact(%Forwarding{} = f),
    do:
      Map.take(f, [
        :id,
        :tenant_id,
        :source_account_id,
        :target_account_id,
        :target_address,
        :mode,
        :only_classes,
        :enabled
      ])

  defp compact(%Tenant{} = t), do: Map.take(t, [:id, :shard])

  defp compact(%DeliveryRule{} = r) do
    rule =
      if r.enabled do
        case Rules.compile(DeliveryRule.to_map(r)) do
          {:ok, rule} ->
            rule

          {:error, reason} ->
            Logger.warning("delivery rule #{r.id} (#{r.name}) skipped: #{reason}")
            nil
        end
      end

    %{key: {DeliveryRule.scope_key(r), r.id}, rule: rule}
  end

  defp safely(st, fun) do
    fun.(st)
  rescue
    e ->
      Logger.warning("directory cache refresh failed: #{Exception.message(e)}")
      st
  end

  defp fingerprint(account, password),
    do:
      :crypto.mac(:hmac, :sha256, :persistent_term.get(@secret), [
        account.password_hash || "",
        0,
        password
      ])

  defp max_time(nil, t), do: t
  defp max_time(a, b), do: if(DateTime.compare(a, b) == :lt, do: b, else: a)

  defp schedule(msg, ms), do: Process.send_after(self(), msg, ms)
  defp now, do: System.monotonic_time(:millisecond)

  defp config(key, default),
    do: Application.get_env(:mailstore, :directory_cache, []) |> Keyword.get(key, default)
end

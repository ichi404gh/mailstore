defmodule Mailstore.Shards do
  @moduledoc """
  The one seam between the directory and mail storage: tenant -> shard -> repo.

  A shard is a Postgres database holding the mail of some tenants (folders, messages,
  bodies, expunge log, delivery keys), with its own schema (`priv/shard/migrations`).
  The directory (`Mailstore.Repo`, a database of its own) lists the shards (`shards`)
  and where each tenant lives (`tenants.shard`); every node caches both
  (`Directory.Cache`, and the repos started here).

  Each shard gets an Ecto repo module of its own, created at runtime
  (`Mailstore.ShardRepo.S<id>`), so the code that takes a `repo` module (commands,
  queries, partitions) works on any shard unchanged. Their telemetry events are the
  directory repo's (`[:mailstore, :repo, :query]`), so query counting sees all of them.

  Folder ids never collide across shards: shard `n` allocates them from `n <<< 40`,
  so a tenant moved to another shard keeps its folder ids and its messages keep
  pointing at them.

  The first shard of an installation comes from `config :mailstore, :bootstrap_shards`
  (registered by `mix mailstore.migrate`); more are added with `register/3`.

  Moving a tenant without downtime: `move_tenant/2` (see there).
  """
  import Bitwise
  require Logger

  alias Mailstore.{Directory, Repo}
  alias Mailstore.Mailbox.Partitions
  alias Mailstore.Shards.Registry

  defmodule TenantMoved do
    @moduledoc "A write hit a folder fenced off by a tenant move: look the shard up again and retry."
    defexception [:message]
  end

  @doc "Repo of the shard a tenant lives on."
  def repo(tenant_id), do: repo_for(Directory.shard_of(tenant_id))

  @doc "Repo of a shard."
  def repo_for(shard_id) do
    # A shard registered on another node in the last seconds: look once more.
    Registry.repo(shard_id) || (Registry.reload() == :ok && Registry.repo(shard_id)) ||
      raise "unknown shard #{shard_id}"
  end

  @doc "Repos of all shards (partition upkeep, retention, migrations)."
  def all, do: Registry.repos()

  @doc "Pick up shards and tenant placements changed in the directory, on every node."
  def refresh do
    for n <- Mailstore.Mailbox.Placement.members(),
        n != node(),
        do: :erpc.cast(n, __MODULE__, :refresh_local, [])

    refresh_local()
  end

  @doc false
  def refresh_local do
    :ok = Registry.reload()
    # Not running under `mix mailstore.migrate`, which starts the repos only.
    if Process.whereis(Directory.Cache), do: :ok = Directory.Cache.refresh()
    :ok
  end

  @doc """
  Add a shard to a running cluster: no restart, every node opens a pool to it.

  The database must exist and be on the current schema (deploys run
  `mix mailstore.migrate`, which migrates every registered shard); a shard with
  pending migrations is refused with `{:error, {:pending_migrations, versions}}`.
  For development and tests, `create: true` creates the database (on a running
  Postgres server, like `mix ecto.create`) and `migrate: true` migrates it.

  The shard gets its folder id range (`n <<< 40`). Returns `{:ok, shard_id}`.
  """
  def register(name, url, opts \\ []) do
    if Keyword.get(opts, :create, false) do
      case Ecto.Adapters.Postgres.storage_up(Ecto.Repo.Supervisor.parse_url(url)) do
        :ok -> :ok
        {:error, :already_up} -> :ok
        {:error, reason} -> raise "cannot create #{name}: #{inspect(reason)}"
      end
    end

    %{rows: [[id]]} =
      Repo.query!(
        """
        INSERT INTO shards (id, name, url, pool_size, inserted_at, updated_at)
        VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM shards), $1, $2, $3, now(), now())
        ON CONFLICT (name) DO UPDATE SET url = EXCLUDED.url, updated_at = now()
        RETURNING id
        """,
        [name, url, Keyword.get(opts, :pool_size, 10)]
      )

    :ok = Registry.reload()
    repo = repo_for(id)
    if Keyword.get(opts, :migrate, false), do: migrate(repo)

    case pending_migrations(repo) do
      [] ->
        id_ranges(repo, id)

        Partitions.ensure(repo)
        refresh()
        {:ok, id}

      pending ->
        Repo.query!("DELETE FROM shards WHERE id = $1", [id])
        :ok = Registry.reload()
        {:error, {:pending_migrations, pending}}
    end
  end

  @doc """
  Register the shards of `config :mailstore, :bootstrap_shards` that the directory does
  not list yet (`[%{name, url}]`, plus `create: true` to create the database: for
  development and tests). Migrates them. Returns the ids registered.
  """
  def bootstrap do
    known = MapSet.new(List.flatten(Repo.query!("SELECT name FROM shards").rows))

    for shard <- Application.get_env(:mailstore, :bootstrap_shards, []),
        not MapSet.member?(known, shard.name) do
      opts = [
        create: Map.get(shard, :create, false),
        migrate: true,
        pool_size: Map.get(shard, :pool_size, 10)
      ]

      {:ok, id} = register(shard.name, shard.url, opts)
      id
    end
  end

  @doc """
  Bring every registered shard to the current shard schema (what
  `mix mailstore.migrate` runs at deploy). Returns `[{repo, versions_applied}]`.
  """
  def migrate_all do
    for {id, repo} <- Registry.entries() do
      applied = migrate(repo)
      id_ranges(repo, id)
      {repo, applied}
    end
  end

  # Ids that travel with a moving tenant come from the shard's own range (`id <<< 40`):
  # folders (so UIDs and MODSEQs move unchanged) and cold packs.
  defp id_ranges(repo, id) do
    for seq <- ~w(folders_id_seq cold_packs_id_seq) do
      repo.query!(
        "SELECT setval('#{seq}', GREATEST((SELECT last_value FROM #{seq}), $1))",
        [id <<< 40]
      )
    end
  end

  @doc "Migration versions not yet applied on a shard's database."
  def pending_migrations(repo) do
    for {:down, version, _name} <- Ecto.Migrator.migrations(repo, migrations_path()), do: version
  end

  defp migrate(repo), do: Ecto.Migrator.run(repo, migrations_path(), :up, all: true, log: false)

  defp migrations_path, do: Application.app_dir(:mailstore, "priv/shard/migrations")

  @doc "Move a tenant to another shard without downtime (`Mailstore.Shards.Move`)."
  defdelegate move_tenant(tenant_id, target_shard_id), to: Mailstore.Shards.Move, as: :run

  @doc "Remove a moved tenant's rows from the shard it left."
  defdelegate cleanup_moved(tenant_id, shard_id), to: Mailstore.Shards.Move, as: :cleanup

  @doc false
  def repo_config(%{url: url, pool_size: pool_size}),
    do: [
      url: url,
      pool_size: pool_size,
      socket_options: [buffer: 65_536],
      # After a switchover the old primary comes back as a replica, and the proxy may
      # still route a reconnect there until its health check notices: drop such a
      # connection instead of failing every write on it.
      disconnect_on_error_codes: [:read_only_sql_transaction],
      log: false,
      telemetry_prefix: [:mailstore, :repo]
    ]
end

defmodule Mailstore.Shards.Registry do
  @moduledoc false
  # Starts a repo per shard listed in the directory and maps shard id -> repo module.
  use GenServer
  require Logger

  import Ecto.Query
  alias Mailstore.Repo

  @table :mailstore_shards
  @supervisor Mailstore.Shards.RepoSupervisor
  # Registration tells the cluster at once; this catches a node that missed it.
  @reload_every 30_000

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def repo(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{_, repo}] -> repo
      [] -> nil
    end
  end

  def repos, do: :ets.select(@table, [{{:_, :"$1"}, [], [:"$1"]}])

  def entries, do: :ets.tab2list(@table) |> Enum.sort()

  def reload, do: GenServer.call(__MODULE__, :reload, 60_000)

  # Loaded before init returns: the application is up only once every shard has a repo.
  @impl true
  def init(nil) do
    :ets.new(@table, [:named_table, :public, read_concurrency: true])
    Process.send_after(self(), :reload, @reload_every)
    {:ok, load(%{})}
  end

  @impl true
  def handle_info(:reload, st) do
    Process.send_after(self(), :reload, @reload_every)
    {:noreply, load(st)}
  end

  @impl true
  def handle_call(:reload, _from, st), do: {:reply, :ok, load(st)}

  # Every shard with a url gets a repo module and a pool; shard 1 without one is Repo.
  defp load(st) do
    case read() do
      {:ok, rows} -> apply_rows(st, rows)
      # Keep what is known: a failed read must not close the pools of live shards.
      :error -> st
    end
  end

  defp read do
    {:ok, Repo.all(from(s in "shards", select: %{id: s.id, url: s.url, pool_size: s.pool_size}))}
  rescue
    e ->
      Logger.warning("shards: registry not read: #{Exception.message(e)}")
      :error
  end

  defp apply_rows(st, rows) do
    # Shards no longer listed: close their pools.
    listed = MapSet.new(rows, & &1.id)

    st =
      Enum.reduce(st, st, fn {id, _url}, st ->
        if MapSet.member?(listed, id) do
          st
        else
          if pid = Process.whereis(module(id)),
            do: DynamicSupervisor.terminate_child(@supervisor, pid)

          :ets.delete(@table, id)
          Map.delete(st, id)
        end
      end)

    Enum.reduce(rows, st, fn
      %{id: id, url: url} = row, st ->
        if st[id] == url do
          st
        else
          repo = module(id)
          if pid = Process.whereis(repo), do: DynamicSupervisor.terminate_child(@supervisor, pid)

          {:ok, _} =
            DynamicSupervisor.start_child(@supervisor, {repo, Mailstore.Shards.repo_config(row)})

          :ets.insert(@table, {id, repo})
          Map.put(st, id, url)
        end
    end)
  end

  defp module(id) do
    repo = Module.concat(Mailstore.ShardRepo, "S#{id}")

    unless Code.ensure_loaded?(repo) do
      Module.create(
        repo,
        quote(do: use(Ecto.Repo, otp_app: :mailstore, adapter: Ecto.Adapters.Postgres)),
        Macro.Env.location(__ENV__)
      )
    end

    repo
  end
end

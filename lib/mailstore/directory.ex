defmodule Mailstore.Directory do
  @moduledoc """
  Context: who exists and where they live (tenants, domains, accounts, routing rules).

  In production this is a global database owned by the control plane. Every node keeps
  it in ETS (`Directory.Cache`), so recipient lookup at delivery and LOGIN cost
  microseconds and no database round trip; until the cache has loaded, reads go to the
  database.

  This module is the context's public API; `Directory.Schemas.*` and
  `Directory.Queries` are internal.
  """
  import Ecto.Query, only: [from: 2]
  alias Mailstore.{Mailbox, Repo}
  alias Mailstore.Directory.{Cache, Password, Queries}
  alias Mailstore.Core.Rules
  alias Mailstore.Directory.Schemas.{Account, DeliveryRule, Domain, Forwarding, Tenant}

  ## Tenants and domains

  def create_tenant(attrs) do
    with {:ok, tenant} <- %Tenant{} |> Tenant.changeset(Map.new(attrs)) |> Repo.insert() do
      :ok = Cache.put_tenant(tenant)
      {:ok, tenant}
    end
  end

  def get_tenant!(id), do: Repo.get!(Tenant, id)

  @doc "The shard a tenant's mail lives on (`Mailstore.Shards`): from the cache, else the database."
  def shard_of(tenant_id) do
    (Cache.loaded?() && Cache.tenant_shard(tenant_id)) ||
      Repo.one!(from(t in Tenant, where: t.id == ^tenant_id, select: t.shard))
  end

  def create_domain(%Tenant{id: tenant_id}, name) do
    %Domain{} |> Domain.changeset(%{tenant_id: tenant_id, name: name}) |> Repo.insert()
  end

  @doc """
  A domain without an owner: accounts of any tenant can live in it, each created with
  its tenant (`create_account(domain, local, tenant: tenant)`). For master inboxes of
  all customers on one domain. Deleting a tenant removes its accounts there, not the
  domain.
  """
  def create_shared_domain(name) do
    %Domain{} |> Domain.changeset(%{tenant_id: nil, name: name}) |> Repo.insert()
  end

  def get_domain_by_name(name), do: Repo.get_by(Domain, name: String.downcase(name))

  ## Accounts

  @doc """
  Create an account and its default folders. In production these are two databases
  (directory, then shard), so folder creation must be idempotent: it is.
  Pass `password:` to set a password (stored as a hash), or a ready `password_hash:`
  (bulk seeding hashes once).

  The account belongs to the domain's tenant. In a shared domain
  (`create_shared_domain/1`) pass the tenant: `tenant: %Tenant{}` (or `tenant_id:`);
  without one it is `{:error, :tenant_required}`. A tenant other than the owner of an
  owned domain is `{:error, :tenant_mismatch}`.
  """
  def create_account(%Domain{} = domain, local_part, attrs \\ %{}) do
    {password, attrs} = attrs |> Map.new() |> Map.pop(:password)
    {tenant, attrs} = Map.pop(attrs, :tenant)
    {tenant_id, attrs} = Map.pop(attrs, :tenant_id, tenant && tenant.id)

    case account_tenant(domain, tenant_id) do
      {:ok, tenant_id} -> insert_account(domain, local_part, tenant_id, password, attrs)
      error -> error
    end
  end

  defp account_tenant(%Domain{tenant_id: nil}, nil), do: {:error, :tenant_required}
  defp account_tenant(%Domain{tenant_id: nil}, tenant_id), do: {:ok, tenant_id}
  defp account_tenant(%Domain{tenant_id: owner}, nil), do: {:ok, owner}
  defp account_tenant(%Domain{tenant_id: owner}, owner), do: {:ok, owner}
  defp account_tenant(%Domain{}, _other), do: {:error, :tenant_mismatch}

  defp insert_account(domain, local_part, tenant_id, password, attrs) do
    attrs =
      attrs
      |> then(fn a ->
        if password, do: Map.put(a, :password_hash, Password.hash(password)), else: a
      end)
      |> Map.merge(%{
        tenant_id: tenant_id,
        domain_id: domain.id,
        local_part: local_part,
        address: "#{local_part}@#{domain.name}"
      })

    with {:ok, account} <- %Account{} |> Account.changeset(attrs) |> Repo.insert() do
      :ok = Mailbox.create_default_folders(account)
      :ok = Cache.put_account(account)
      {:ok, account}
    end
  end

  @doc "Account by id: a map with id, tenant_id, domain_id, address, status, kind."
  def get_account(id) do
    if Cache.loaded?(), do: Cache.account(id), else: Repo.get(Account, id)
  end

  @doc "Hash a password the way accounts store it; `iterations` overrides the configured cost."
  defdelegate hash_password(password, iterations \\ nil), to: Password, as: :hash

  @doc "IMAP LOGIN: {:ok, account} or {:error, :invalid_credentials} (no hint which part was wrong)."
  def authenticate(address, password) do
    with {:ok, account} <- lookup_account(address),
         true <- verify(account, password) do
      {:ok, account}
    else
      _ -> {:error, :invalid_credentials}
    end
  end

  # PBKDF2 only on a cache miss; only successes are remembered.
  defp verify(account, password) do
    cond do
      Cache.loaded?() and Cache.auth_ok?(account, password) ->
        true

      Password.verify(account.password_hash, password) ->
        if Cache.loaded?(), do: Cache.remember_auth(account, password)
        true

      true ->
        false
    end
  end

  def lookup_account(address) do
    account =
      if Cache.loaded?(),
        do: Cache.lookup(address),
        else: address |> Queries.active_account_by_address() |> Repo.one()

    case account do
      %{status: "active"} -> {:ok, account}
      _ -> {:error, :unknown_recipient}
    end
  end

  ## Routing rules

  @doc "Forward or copy mail from `source` to another account or an external address."
  def create_forwarding(%Account{} = source, target, attrs \\ %{}) do
    target_attrs =
      case target do
        %Account{id: id} -> %{target_account_id: id}
        address when is_binary(address) -> %{target_address: address}
      end

    attrs =
      attrs
      |> Map.new()
      |> Map.merge(target_attrs)
      |> Map.merge(%{tenant_id: source.tenant_id, source_account_id: source.id})

    with {:ok, fwd} <- %Forwarding{} |> Forwarding.changeset(attrs) |> Repo.insert() do
      :ok = Cache.put_forwarding(fwd)
      {:ok, fwd}
    end
  end

  @doc "Enabled rules of an account; with `class`, only those that fire for that message class."
  def forwardings_for(account_id, class \\ nil) do
    rules =
      if Cache.loaded?(),
        do:
          account_id |> Cache.forwardings() |> Enum.filter(& &1.enabled) |> Enum.sort_by(& &1.id),
        else: account_id |> Queries.enabled_forwardings_of() |> Repo.all()

    if class, do: Enum.filter(rules, &Forwarding.applies?(&1, class)), else: rules
  end

  ## Delivery rules (what the Sieve scripts did, as data: Mailstore.Core.Rules)

  @doc "Compiled rules that apply to a mailbox: global, its tenant's and its own."
  def rules_for(%{id: account_id, tenant_id: tenant_id}) do
    if Cache.loaded?() do
      Cache.rules(tenant_id, account_id)
    else
      import Ecto.Query

      from(r in DeliveryRule,
        where:
          r.enabled and
            (r.scope == "global" or (r.scope == "tenant" and r.tenant_id == ^tenant_id) or
               (r.scope == "account" and r.account_id == ^account_id))
      )
      |> Repo.all()
      |> Enum.flat_map(fn r ->
        case Rules.compile(DeliveryRule.to_map(r)) do
          {:ok, rule} -> [rule]
          {:error, _} -> []
        end
      end)
    end
  end

  @doc """
  Add a rule. `attrs`: `scope` ("global" | "tenant" | "account"), `tenant_id` or
  `account_id`, `stage`, `position`, `name`, `spec` (`%{"test" => ..., "actions" => [...]}`).
  An invalid spec is rejected with the compiler's reason.
  """
  def create_rule(attrs) do
    with {:ok, rule} <- %DeliveryRule{} |> DeliveryRule.changeset(Map.new(attrs)) |> Repo.insert() do
      :ok = Cache.put_rule(rule)
      {:ok, rule}
    end
  end

  def update_rule(%DeliveryRule{} = rule, attrs) do
    with {:ok, rule} <- rule |> DeliveryRule.changeset(Map.new(attrs)) |> Repo.update() do
      :ok = Cache.put_rule(rule)
      {:ok, rule}
    end
  end

  def delete_rule(%DeliveryRule{} = rule) do
    with {:ok, rule} <- Repo.delete(rule) do
      :ok = Cache.delete_rule(rule)
      {:ok, rule}
    end
  end

  @doc """
  Replace all global rules with `rules` (maps with `name`, `stage`, `position`, `test`,
  `actions`, as in `priv/delivery_rules/*.json`) in one transaction. Either every rule
  is valid and the set is swapped, or nothing changes: `{:error, name, reason}`.
  """
  def import_global_rules(rules) when is_list(rules) do
    result =
      Repo.transaction(fn ->
        import Ecto.Query
        Repo.delete_all(from(r in DeliveryRule, where: r.scope == "global"))

        for r <- rules do
          attrs = %{
            scope: "global",
            stage: r["stage"],
            position: r["position"],
            name: r["name"],
            spec: Map.take(r, ["test", "actions"]),
            enabled: Map.get(r, "enabled", true)
          }

          case %DeliveryRule{} |> DeliveryRule.changeset(attrs) |> Repo.insert() do
            {:ok, rule} -> rule
            {:error, cs} -> Repo.rollback({r["name"], inspect(cs.errors)})
          end
        end
      end)

    case result do
      {:ok, inserted} ->
        :ok = Cache.reload_rules()
        {:ok, length(inserted)}

      {:error, {name, reason}} ->
        {:error, name, reason}
    end
  end
end

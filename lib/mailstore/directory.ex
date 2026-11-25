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
  alias Mailstore.{Mailbox, Repo}
  alias Mailstore.Directory.{Cache, Password, Queries}
  alias Mailstore.Directory.Schemas.{Account, Domain, Forwarding, Tenant}

  ## Tenants and domains

  def create_tenant(attrs), do: %Tenant{} |> Tenant.changeset(Map.new(attrs)) |> Repo.insert()

  def get_tenant!(id), do: Repo.get!(Tenant, id)

  def create_domain(%Tenant{id: tenant_id}, name) do
    %Domain{} |> Domain.changeset(%{tenant_id: tenant_id, name: name}) |> Repo.insert()
  end

  def get_domain_by_name(name), do: Repo.get_by(Domain, name: String.downcase(name))

  ## Accounts

  @doc """
  Create an account and its default folders. In production these are two databases
  (directory, then shard), so folder creation must be idempotent: it is.
  Pass `password:` to set a password (stored as a hash), or a ready `password_hash:`
  (bulk seeding hashes once).
  """
  def create_account(%Domain{} = domain, local_part, attrs \\ %{}) do
    {password, attrs} = attrs |> Map.new() |> Map.pop(:password)

    attrs =
      attrs
      |> then(fn a ->
        if password, do: Map.put(a, :password_hash, Password.hash(password)), else: a
      end)
      |> Map.merge(%{
        tenant_id: domain.tenant_id,
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
end

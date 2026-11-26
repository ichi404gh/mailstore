defmodule Mailstore.Fixtures do
  alias Mailstore.{Directory, Mailbox}
  alias Mailstore.Core.NewMessage

  def tenant_fixture(name \\ "acme") do
    {:ok, tenant} = Directory.create_tenant(%{name: name})
    tenant
  end

  def account_fixture(address, opts \\ []) do
    [local, domain_name] = String.split(address, "@")
    tenant = opts[:tenant] || tenant_fixture()

    domain =
      Directory.get_domain_by_name(domain_name) ||
        elem(Directory.create_domain(tenant, domain_name), 1)

    {:ok, account} =
      Directory.create_account(domain, local, Map.new(Keyword.take(opts, [:kind, :password])))

    account
  end

  @doc "The repo of the shard an account's mail lives on."
  def shard_repo(account), do: Mailstore.Shards.repo(account.tenant_id)

  def folder_id(account, name) do
    account.tenant_id
    |> Mailbox.list_folders(account.id)
    |> Enum.find(&(&1.name == name))
    |> Map.fetch!(:id)
  end

  def raw_message(opts \\ []) do
    id = System.unique_integer([:positive])

    headers =
      [
        {"From", opts[:from] || "Alice Sender <alice@example.org>"},
        {"To", opts[:to] || "bob@acme.test"},
        {"Subject", opts[:subject] || "Quick question #{id}"},
        {"Date", "Fri, 20 Nov 2026 12:00:00 +0000"},
        {"Message-ID", opts[:message_id] || "<msg-#{id}@example.org>"},
        {"In-Reply-To", opts[:in_reply_to]},
        {"References", opts[:references]}
      ]
      |> Enum.reject(fn {_, v} -> is_nil(v) end)
      |> Enum.map_join("\r\n", fn {k, v} -> "#{k}: #{v}" end)

    body = opts[:body] || String.duplicate("Hi Bob, just following up on my last note. ", 30)
    headers <> "\r\n\r\n" <> body <> "\r\n"
  end

  def new_message(opts \\ []), do: NewMessage.from_raw(raw_message(opts), opts)
end

defmodule Mailstore.Directory.Schemas.Account do
  use Mailstore.Schema

  @kinds ~w(mailbox master_inbox)

  schema "accounts" do
    belongs_to :tenant, Mailstore.Directory.Schemas.Tenant
    belongs_to :domain, Mailstore.Directory.Schemas.Domain
    field :local_part, :string
    field :address, :string
    field :password_hash, :string, redact: true
    field :kind, :string, default: "mailbox"
    field :retention_class, :string, default: "standard"
    field :status, :string, default: "active"
    field :quota_bytes, :integer
    timestamps()
  end

  def changeset(account, attrs) do
    account
    |> cast(attrs, [
      :tenant_id,
      :domain_id,
      :local_part,
      :address,
      :password_hash,
      :kind,
      :retention_class,
      :status,
      :quota_bytes
    ])
    |> validate_required([:tenant_id, :domain_id, :local_part, :address])
    |> validate_inclusion(:kind, @kinds)
    |> update_change(:address, &String.downcase/1)
    |> unique_constraint(:address)
  end
end

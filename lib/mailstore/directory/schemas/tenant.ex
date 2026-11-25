defmodule Mailstore.Directory.Schemas.Tenant do
  use Mailstore.Schema

  schema "tenants" do
    field :name, :string
    field :shard, :integer, default: 0
    has_many :domains, Mailstore.Directory.Schemas.Domain
    timestamps()
  end

  def changeset(tenant, attrs) do
    tenant
    |> cast(attrs, [:id, :name, :shard])
    |> validate_required([:name])
  end
end

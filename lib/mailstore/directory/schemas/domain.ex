defmodule Mailstore.Directory.Schemas.Domain do
  use Mailstore.Schema

  schema "domains" do
    belongs_to :tenant, Mailstore.Directory.Schemas.Tenant
    field :name, :string
    field :status, :string, default: "active"
    timestamps()
  end

  def changeset(domain, attrs) do
    domain
    |> cast(attrs, [:tenant_id, :name, :status])
    |> validate_required([:tenant_id, :name])
    |> update_change(:name, &String.downcase/1)
    |> unique_constraint(:name)
  end
end

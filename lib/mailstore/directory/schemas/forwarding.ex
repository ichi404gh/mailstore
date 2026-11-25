defmodule Mailstore.Directory.Schemas.Forwarding do
  use Mailstore.Schema

  schema "forwardings" do
    belongs_to :tenant, Mailstore.Directory.Schemas.Tenant
    belongs_to :source_account, Mailstore.Directory.Schemas.Account
    belongs_to :target_account, Mailstore.Directory.Schemas.Account
    field :target_address, :string
    field :mode, :string, default: "copy"
    field :only_classes, {:array, :string}
    field :enabled, :boolean, default: true
    timestamps()
  end

  def changeset(fwd, attrs) do
    fwd
    |> cast(attrs, [
      :tenant_id,
      :source_account_id,
      :target_account_id,
      :target_address,
      :mode,
      :only_classes,
      :enabled
    ])
    |> validate_required([:tenant_id, :source_account_id])
    |> validate_inclusion(:mode, ~w(copy redirect))
    |> check_constraint(:target_account_id, name: :exactly_one_target)
  end

  @doc "Whether this rule (struct or cached map) fires for a message of the given class."
  def applies?(%{enabled: false}, _class), do: false
  def applies?(%{only_classes: nil}, _class), do: true
  def applies?(%{only_classes: []}, _class), do: true
  def applies?(%{only_classes: classes}, class), do: Atom.to_string(class) in classes
end

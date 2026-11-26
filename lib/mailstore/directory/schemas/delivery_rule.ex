defmodule Mailstore.Directory.Schemas.DeliveryRule do
  @moduledoc "A delivery rule row; `spec` is `%{\"test\" => ..., \"actions\" => [...]}` (see `Mailstore.Core.Rules`)."
  use Mailstore.Schema
  alias Mailstore.Core.Rules

  schema "delivery_rules" do
    field :scope, :string
    belongs_to :tenant, Mailstore.Directory.Schemas.Tenant
    belongs_to :account, Mailstore.Directory.Schemas.Account
    field :stage, :string
    field :position, :integer
    field :name, :string
    field :spec, :map
    field :enabled, :boolean, default: true
    timestamps()
  end

  def changeset(rule, attrs) do
    rule
    |> cast(attrs, [:scope, :tenant_id, :account_id, :stage, :position, :name, :spec, :enabled])
    |> validate_required([:scope, :stage, :position, :name, :spec])
    |> validate_inclusion(:scope, ~w(global tenant account))
    |> validate_inclusion(:stage, ~w(detect forward))
    |> validate_spec()
    |> check_constraint(:scope, name: :scope_target)
  end

  # The same compiler the cache uses: an invalid rule never reaches the table.
  defp validate_spec(changeset) do
    case Rules.compile(to_map(apply_changes(changeset))) do
      {:ok, _} -> changeset
      {:error, reason} -> add_error(changeset, :spec, reason)
    end
  end

  @doc "The map `Mailstore.Core.Rules.compile/1` takes."
  def to_map(rule) do
    Map.merge(rule.spec || %{}, %{
      "id" => rule.id,
      "name" => rule.name,
      "stage" => rule.stage,
      "position" => rule.position,
      "scope" => scope_key(rule)
    })
  end

  def scope_key(%{scope: "global"}), do: :global
  def scope_key(%{scope: "tenant", tenant_id: id}), do: {:tenant, id}
  def scope_key(%{scope: "account", account_id: id}), do: {:account, id}
end

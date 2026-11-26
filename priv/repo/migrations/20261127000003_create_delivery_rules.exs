defmodule Mailstore.Repo.Migrations.CreateDeliveryRules do
  @moduledoc """
  Delivery rules (`Mailstore.Core.Rules`): what the per-mailbox Sieve scripts did, as
  data. A rule applies to everyone (`global`), to one tenant's mailboxes (`tenant`, e.g.
  a customer's warmup filters) or to one mailbox (`account`, e.g. its skip sequence).
  `spec` holds the test and the actions; rules of all scopes are merged by `position`.
  """
  use Ecto.Migration

  def change do
    create table(:delivery_rules) do
      add :scope, :text, null: false
      add :tenant_id, references(:tenants, on_delete: :delete_all)
      add :account_id, references(:accounts, on_delete: :delete_all)
      add :stage, :text, null: false
      add :position, :integer, null: false
      add :name, :text, null: false
      add :spec, :map, null: false
      add :enabled, :boolean, null: false, default: true
      timestamps()
    end

    create constraint(:delivery_rules, :scope_target,
             check: """
             (scope = 'global' AND tenant_id IS NULL AND account_id IS NULL) OR
             (scope = 'tenant' AND tenant_id IS NOT NULL AND account_id IS NULL) OR
             (scope = 'account' AND account_id IS NOT NULL)
             """
           )

    create constraint(:delivery_rules, :stage_known, check: "stage IN ('detect', 'forward')")
    create index(:delivery_rules, [:updated_at])
  end
end

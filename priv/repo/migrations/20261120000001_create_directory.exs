defmodule Mailstore.Repo.Migrations.CreateDirectory do
  @moduledoc """
  Directory: who exists and where they live. In production this is one global
  database (fed by the Django control plane) that every node mirrors into ETS.
  The prototype keeps it in the same database as the mail shard.
  """
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS citext", "DROP EXTENSION IF EXISTS citext"

    # tenant = a customer account. Its shard is the unit of data placement.
    create table(:tenants) do
      add :name, :text, null: false
      add :shard, :integer, null: false, default: 1
      timestamps()
    end

    create table(:domains) do
      add :tenant_id, references(:tenants, on_delete: :delete_all), null: false
      add :name, :citext, null: false
      add :status, :text, null: false, default: "active"
      timestamps()
    end

    create unique_index(:domains, [:name])
    create index(:domains, [:tenant_id])

    create table(:accounts) do
      add :tenant_id, references(:tenants, on_delete: :delete_all), null: false
      add :domain_id, references(:domains, on_delete: :delete_all), null: false
      add :local_part, :citext, null: false
      add :address, :citext, null: false
      add :password_hash, :text
      # mailbox | master_inbox
      add :kind, :text, null: false, default: "mailbox"
      add :retention_class, :text, null: false, default: "standard"
      add :status, :text, null: false, default: "active"
      add :quota_bytes, :bigint
      timestamps()
    end

    create unique_index(:accounts, [:address])
    create unique_index(:accounts, [:domain_id, :local_part])
    create index(:accounts, [:tenant_id])

    # Routing rules applied at delivery time: master-inbox copies and external forwards.
    create table(:forwardings) do
      add :tenant_id, references(:tenants, on_delete: :delete_all), null: false
      add :source_account_id, references(:accounts, on_delete: :delete_all), null: false
      add :target_account_id, references(:accounts, on_delete: :delete_all)
      add :target_address, :citext
      # copy: keep the original and add a copy; redirect: do not keep the original
      add :mode, :text, null: false, default: "copy"
      # NULL = all classes; otherwise e.g. {reply} so warmup never reaches the master inbox
      add :only_classes, {:array, :text}
      add :enabled, :boolean, null: false, default: true
      timestamps()
    end

    create index(:forwardings, [:source_account_id])

    create constraint(:forwardings, :exactly_one_target,
             check: "(target_account_id IS NULL) <> (target_address IS NULL)"
           )

    create constraint(:forwardings, :valid_mode, check: "mode IN ('copy', 'redirect')")
  end
end

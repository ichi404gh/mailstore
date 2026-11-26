defmodule Mailstore.Shard.Migrations.AddFolderFenced do
  @moduledoc """
  Set on the source shard when a tenant has moved away (`Mailstore.Shards.Move`):
  writers refuse fenced folders and retry on the tenant's new shard.
  """
  use Ecto.Migration

  def change do
    alter table(:folders), do: add(:fenced, :boolean, null: false, default: false)
  end
end

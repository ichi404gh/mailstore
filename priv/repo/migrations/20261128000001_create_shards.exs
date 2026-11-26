defmodule Mailstore.Repo.Migrations.CreateShards do
  @moduledoc """
  The shard registry (`Mailstore.Shards`): one row per Postgres database that holds
  mail, and `tenants.shard` pointing at it. Shards have their own schema
  (`priv/shard/migrations`); `mix mailstore.migrate` registers the bootstrap shard
  and migrates them all.
  """
  use Ecto.Migration

  def change do
    create table(:shards, primary_key: false) do
      add :id, :integer, primary_key: true
      add :name, :text, null: false
      add :url, :text, null: false
      add :pool_size, :integer, null: false, default: 10
      timestamps()
    end

    create unique_index(:shards, [:name])

    alter table(:tenants) do
      modify :shard, references(:shards, type: :integer), from: :integer
    end
  end
end

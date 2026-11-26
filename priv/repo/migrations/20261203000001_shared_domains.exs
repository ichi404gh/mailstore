defmodule Mailstore.Repo.Migrations.SharedDomains do
  @moduledoc """
  A domain without an owner (`tenant_id` NULL) is shared: accounts of many tenants
  live in it, each with its own tenant (master inboxes of all customers on one
  domain). An account's tenant decides its shard, rules and moves, as before.
  """
  use Ecto.Migration

  def up, do: execute("ALTER TABLE domains ALTER COLUMN tenant_id DROP NOT NULL")
  def down, do: execute("ALTER TABLE domains ALTER COLUMN tenant_id SET NOT NULL")
end

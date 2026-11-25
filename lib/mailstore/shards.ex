defmodule Mailstore.Shards do
  @moduledoc """
  The one seam between the directory and mail storage: tenant -> shard repo.

  The prototype has a single shard. In production this reads `tenants.shard` from the
  directory cache and returns a dynamic repo for that shard. Moving a tenant between
  shards = copy its rows, then flip the mapping here. Clients never notice.
  """

  def repo(_tenant_id), do: Mailstore.Repo

  def all, do: [Mailstore.Repo]
end

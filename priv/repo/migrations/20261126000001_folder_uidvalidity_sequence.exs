defmodule Mailstore.Repo.Migrations.FolderUidvaliditySequence do
  @moduledoc """
  UIDVALIDITY of new folders comes from a sequence, not from the clock.

  A folder created again under the same name (DELETE + CREATE, or RENAME of another
  folder into it) must get a different UIDVALIDITY, or clients keep their cached UIDs
  for the new folder (RFC 3501 2.3.1.1). Seconds since the epoch repeat within one
  second; the sequence never repeats. It starts at the current epoch, so new values are
  above the epoch-based UIDVALIDITYs imported from other servers.

  Per shard. When a tenant moves between shards, advance the target's sequence to at
  least the source's value before switching it over.
  """
  use Ecto.Migration

  def up do
    execute "CREATE SEQUENCE folder_uidvalidity MAXVALUE 4294967295"
    execute "SELECT setval('folder_uidvalidity', extract(epoch FROM now())::bigint)"
  end

  def down do
    execute "DROP SEQUENCE folder_uidvalidity"
  end
end

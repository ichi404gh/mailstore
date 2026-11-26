defmodule Mailstore.Repo.Migrations.AddFolderRecentUid do
  @moduledoc """
  First UID not yet claimed as \\Recent (Dovecot keeps the same, `first_recent_uid`, in
  its index). Delivery no longer goes through the account actor, so an actor that
  starts after mail arrived must learn from here which messages are new. The actor
  writes it back when it stops; a crash only makes some messages \\Recent again.
  """
  use Ecto.Migration

  def up do
    alter table(:folders), do: add(:recent_uid, :bigint, null: false, default: 1)
    execute "UPDATE folders SET recent_uid = uidnext"
  end

  def down, do: alter(table(:folders), do: remove(:recent_uid))
end

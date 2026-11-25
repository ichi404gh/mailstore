defmodule Mailstore.Repo.Migrations.DirectoryUpdatedAtIndexes do
  @moduledoc "The directory cache polls for rows changed since its last look."
  use Ecto.Migration

  def change do
    create index(:accounts, [:updated_at])
    create index(:forwardings, [:updated_at])
  end
end

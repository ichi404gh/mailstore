defmodule Mailstore.Mailbox.Schemas.Folder do
  use Mailstore.Schema

  schema "folders" do
    field :tenant_id, :integer
    field :account_id, :integer
    field :name, :string
    field :special_use, :string
    field :uidvalidity, :integer
    field :uidnext, :integer, default: 1
    field :recent_uid, :integer, default: 1
    field :highest_modseq, :integer, default: 1
    field :retained_from_uid, :integer, default: 1
    field :messages_count, :integer, default: 0
    field :unseen_count, :integer, default: 0
    field :subscribed, :boolean, default: true
    field :fenced, :boolean, default: false
    timestamps()
  end
end

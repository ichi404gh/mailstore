defmodule Mailstore.Mailbox.Schemas.Vanished do
  use Mailstore.Schema

  @primary_key false
  schema "vanished" do
    field :folder_id, :integer, primary_key: true
    field :uid, :integer, primary_key: true
    field :modseq, :integer
  end
end

defmodule Mailstore.Mailbox.Schemas.MessageBody do
  use Mailstore.Schema

  @primary_key false
  schema "message_bodies" do
    field :folder_id, :integer, primary_key: true
    field :uid, :integer, primary_key: true
    field :stored_at, :utc_datetime_usec, primary_key: true
    field :codec, :integer
    field :data, :binary
  end
end

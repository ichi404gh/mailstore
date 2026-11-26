defmodule Mailstore.Mailbox.Schemas.Message do
  @moduledoc "Message metadata. The body lives in `message_bodies` under the same key."
  use Mailstore.Schema

  @primary_key false
  schema "messages" do
    field :folder_id, :integer, primary_key: true
    field :uid, :integer, primary_key: true
    field :stored_at, :utc_datetime_usec, primary_key: true
    # Retention policy (Mailstore.Mailbox.Retention), part of the partition key.
    field :retention, :integer, primary_key: true
    field :tenant_id, :integer
    field :modseq, :integer
    field :internal_date, :utc_datetime_usec
    field :size, :integer
    field :flags, :integer, default: 0
    field :keywords, {:array, :string}, default: []
    field :class, :integer, default: 0
    field :message_id, :string
    field :message_id_h, :integer
    field :in_reply_to_h, :integer
    field :thread_id, :integer
    field :from_addr, :string
    field :subject, :string
    field :envelope, :map
    field :bodystructure, :map
  end
end

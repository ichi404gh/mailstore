defmodule Mailstore.Repo do
  use Ecto.Repo, otp_app: :mailstore, adapter: Ecto.Adapters.Postgres
end

defmodule Mailstore.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        Mailstore.Telemetry,
        Mailstore.Repo,
        Mailstore.Directory.Cache,
        Mailstore.Mailbox.Supervisor
      ] ++ if(Mailstore.Imap.enabled?(), do: [Mailstore.Imap], else: [])

    Supervisor.start_link(children, strategy: :one_for_one, name: Mailstore.Supervisor)
  end
end

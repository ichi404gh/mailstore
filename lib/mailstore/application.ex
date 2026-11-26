defmodule Mailstore.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        Mailstore.Telemetry,
        Mailstore.Repo,
        {DynamicSupervisor, name: Mailstore.Shards.RepoSupervisor, strategy: :one_for_one},
        Mailstore.Shards.Registry,
        Mailstore.Directory.Cache,
        Mailstore.Cluster,
        Mailstore.Mailbox.Supervisor
      ] ++
        if(Mailstore.Imap.enabled?(), do: [Mailstore.Imap], else: []) ++
        if(Mailstore.Ingest.enabled?(), do: [Mailstore.Ingest], else: [])

    Supervisor.start_link(children, strategy: :one_for_one, name: Mailstore.Supervisor)
  end
end

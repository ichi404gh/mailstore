defmodule Mailstore.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    # A bad retention configuration stops the start, not the first delivery.
    Mailstore.Mailbox.Retention.load!()

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
        if(Mailstore.Outbound.enabled?(), do: [Mailstore.Outbound], else: []) ++
        if(Mailstore.Ingest.enabled?(), do: [Mailstore.Ingest], else: [])

    if Mailstore.Ingest.enabled?() and not Mailstore.Outbound.enabled?() do
      Logger.warning("outbound is off: forwards to external targets will be dropped")
    end

    Supervisor.start_link(children, strategy: :one_for_one, name: Mailstore.Supervisor)
  end
end

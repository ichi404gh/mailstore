defmodule Mailstore.Imap do
  @moduledoc """
  Context: the IMAP4rev1 front end. Talks to clients, and to the rest of the system
  only through `Mailstore.Directory` (LOGIN) and `Mailstore.Mailbox`.

  Listeners (plain TCP; TLS is terminated in front by HAProxy, see `deploy/haproxy`):
    * `port` - direct connections (tests, local clients);
    * `proxy_port` - for HAProxy: every connection starts with a PROXY protocol header
      carrying the real client address. Only the load balancer may connect here.

  Config (`config :mailstore, :imap`): `port`, `proxy_port`, `num_acceptors`,
  `hibernate_after` (ms an idle connection waits before hibernating), `socket_opts`.

  Internals: `Imap.Connection` (socket process), `Imap.Framing`, `Imap.Parser`,
  `Imap.Session` (state machine), `Imap.Fetch`, `Imap.Search`, `Imap.SequenceSet`,
  `Imap.Format`; the session's seq -> UID view is `Mailstore.Core.UidMap`.
  """
  use Supervisor

  @plain __MODULE__.Listener
  @proxy __MODULE__.ProxyListener

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg) do
    config = Application.fetch_env!(:mailstore, :imap)
    plain = listener(@plain, Keyword.fetch!(config, :port), config, false)

    proxy =
      case Keyword.get(config, :proxy_port) do
        nil -> []
        port -> [listener(@proxy, port, config, true)]
      end

    Supervisor.init([plain | proxy], strategy: :one_for_one)
  end

  defp listener(ref, port, config, proxy?) do
    :ranch.child_spec(
      ref,
      :ranch_tcp,
      %{
        socket_opts: [port: port] ++ Keyword.get(config, :socket_opts, []),
        num_acceptors: Keyword.get(config, :num_acceptors, 16),
        max_connections: :infinity
      },
      Mailstore.Imap.Connection,
      hibernate_after: Keyword.get(config, :hibernate_after, 15_000),
      proxy_protocol: proxy?
    )
  end

  @doc "Port of the direct listener (useful when configured with port 0)."
  def port, do: :ranch.get_port(@plain)

  @doc "Port of the PROXY-protocol listener for HAProxy, or nil."
  def proxy_port do
    if Map.has_key?(:ranch.info(), @proxy), do: :ranch.get_port(@proxy), else: nil
  end

  def enabled?, do: Application.get_env(:mailstore, :imap, []) |> Keyword.get(:enabled, false)

  @doc "Open client connections on all listeners (0 when not running)."
  def connection_count do
    info = :ranch.info()

    for ref <- [@plain, @proxy], reduce: 0 do
      acc ->
        case info do
          %{^ref => %{active_connections: n}} -> acc + n
          _ -> acc
        end
    end
  end

  def connection_pids do
    info = :ranch.info()

    for ref <- [@plain, @proxy],
        Map.has_key?(info, ref),
        pid <- :ranch.procs(ref, :connections),
        do: pid
  end
end

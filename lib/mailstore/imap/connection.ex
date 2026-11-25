defmodule Mailstore.Imap.Connection do
  @moduledoc """
  One process per client connection (ranch protocol). Owns the socket, cuts commands
  out of the byte stream (`Framing`), hands them to `Session`, writes the replies.
  Also receives the account actor's pushes and its DOWN message.

  Memory: most connections sit in IDLE for ~30 minutes, so the process hibernates
  after `:hibernate_after` ms without messages (heap compacted to the minimum). The
  autologout is a timer message rather than a gen_server timeout, because a
  gen_server timeout disables `hibernate_after`.
  """
  @behaviour :ranch_protocol
  use GenServer, restart: :temporary

  alias Mailstore.Imap.{Framing, Session}

  @autologout :timer.minutes(31)

  # `{active, N}` instead of re-arming `{active, :once}` after every command: re-arming
  # makes the inet driver try a read right away, and when nothing is there it keeps the
  # freshly taken receive buffer while the connection idles. With `{active, N}` the
  # buffer is released as soon as the data is delivered. N bounds the mailbox if a
  # client floods us; we re-arm on `{:tcp_passive, socket}`.
  @active_n 100

  @impl :ranch_protocol
  def start_link(ref, transport, opts) do
    {:ok, :proc_lib.spawn_link(__MODULE__, :connect, [ref, transport, opts])}
  end

  @doc false
  def connect(ref, transport, opts) do
    client =
      if Keyword.get(opts, :proxy_protocol, false), do: proxy_client(ref, transport), else: nil

    {:ok, socket} = :ranch.handshake(ref)
    client = client || peer(transport, socket)
    {ok, closed, error, passive} = transport.messages()
    :telemetry.execute([:mailstore, :imap, :connection, :open], %{count: 1}, %{client: client})

    state = %{
      socket: socket,
      transport: transport,
      tags: {ok, closed, error, passive},
      buffer: "",
      framing: Framing.new(),
      session: Session.new(),
      actor: nil,
      actor_ref: nil,
      autologout: nil,
      client: client,
      opened_at: System.monotonic_time()
    }

    transport.send(socket, Session.greeting())
    :ok = transport.setopts(socket, active: @active_n, mode: :binary)

    :gen_server.enter_loop(
      __MODULE__,
      [hibernate_after: Keyword.get(opts, :hibernate_after, 15_000)],
      arm_autologout(state)
    )
  end

  @impl true
  def init(_), do: {:stop, :use_start_link}

  @impl true
  def handle_info({tag, _socket, data}, %{tags: {tag, _, _, _}} = st) do
    st = arm_autologout(%{st | buffer: st.buffer <> data})

    case process(st) do
      {:ok, st} ->
        {:noreply, st}

      {:stop, st} ->
        st.transport.close(st.socket)
        {:stop, :normal, st}
    end
  end

  def handle_info({tag, _socket}, %{tags: {_, _, _, tag}} = st) do
    :ok = st.transport.setopts(st.socket, active: @active_n)
    {:noreply, st}
  end

  def handle_info({tag, _socket}, %{tags: {_, tag, _, _}} = st), do: {:stop, :normal, st}
  def handle_info({tag, _socket, _reason}, %{tags: {_, _, tag, _}} = st), do: {:stop, :normal, st}

  def handle_info({:mailstore, _, _} = push, st) do
    {out, session} = Session.handle_push(push, st.session)
    send_out(st, out)
    {:noreply, track_actor(%{st | session: session})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{actor_ref: ref} = st) do
    {out, session} = Session.handle_actor_down(st.session)
    send_out(st, out)
    st = track_actor(%{st | session: session, actor: nil, actor_ref: nil})

    if Session.logout?(session), do: {:stop, :normal, st}, else: {:noreply, st}
  end

  def handle_info(:autologout, st) do
    send_out(st, "* BYE Autologout\r\n")
    st.transport.close(st.socket)
    {:stop, :normal, st}
  end

  def handle_info(_other, st), do: {:noreply, st}

  @impl true
  def terminate(_reason, st) do
    duration = System.monotonic_time() - st.opened_at
    :telemetry.execute([:mailstore, :imap, :connection, :close], %{duration: duration}, %{})
  end

  # Behind HAProxy the first bytes are a PROXY protocol header with the real client.
  # A LOCAL header (load balancer health check) has no client address.
  defp proxy_client(ref, transport) do
    case :ranch.recv_proxy_header(ref, 5_000) do
      {:ok, %{command: :proxy, src_address: ip}} ->
        ip

      {:ok, %{command: :local}} ->
        nil

      {:error, _} = error ->
        {:ok, socket} = :ranch.handshake(ref)
        transport.close(socket)
        exit({:shutdown, {:bad_proxy_header, error}})

      {:error, _, _} = error ->
        {:ok, socket} = :ranch.handshake(ref)
        transport.close(socket)
        exit({:shutdown, {:bad_proxy_header, error}})
    end
  end

  defp peer(transport, socket) do
    case transport.peername(socket) do
      {:ok, {ip, _port}} -> ip
      _ -> nil
    end
  end

  # Run every complete command in the buffer.
  defp process(st) do
    case Framing.next(st.buffer, st.framing) do
      {:command, command, rest, framing} ->
        {out, session} = Session.handle_command(command, st.session)
        send_out(st, out)
        st = track_actor(%{st | buffer: rest, framing: framing, session: session})
        if Session.logout?(session), do: {:stop, st}, else: process(st)

      {:continue, framing} ->
        send_out(st, "+ Ready for literal data\r\n")
        {:ok, %{st | framing: framing}}

      {:more, framing} ->
        {:ok, %{st | framing: framing}}

      {:error, reason} ->
        send_out(st, ["* BAD ", reason, "\r\n"])
        {:stop, st}
    end
  end

  defp arm_autologout(st) do
    if st.autologout, do: Process.cancel_timer(st.autologout)
    %{st | autologout: Process.send_after(self(), :autologout, @autologout)}
  end

  # Monitor the account actor of the selected folder, so its death triggers a resync.
  defp track_actor(%{session: %{actor: actor}, actor: actor} = st), do: st

  defp track_actor(st) do
    if st.actor_ref, do: Process.demonitor(st.actor_ref, [:flush])
    actor = st.session.actor
    %{st | actor: actor, actor_ref: actor && Process.monitor(actor)}
  end

  defp send_out(_st, []), do: :ok
  defp send_out(_st, ""), do: :ok
  defp send_out(st, out), do: st.transport.send(st.socket, out)
end

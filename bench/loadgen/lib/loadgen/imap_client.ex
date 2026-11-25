defmodule Loadgen.ImapClient do
  @moduledoc """
  Just enough blocking IMAP for the scenarios. A connection is `{:gen_tcp | :ssl, socket}`
  so the same code runs over plain TCP or TLS (`--tls`, e.g. through HAProxy on 993).
  """

  import Kernel, except: [send: 2]

  @timeout 30_000

  def connect(host, port, source_ip, tls? \\ false) do
    opts = [:binary, packet: :line, active: false, nodelay: true, buffer: 4096]
    opts = if source_ip, do: [{:ip, source_ip} | opts], else: opts

    result =
      if tls?,
        do: :ssl.connect(host, port, [verify: :verify_none] ++ opts, @timeout),
        else: :gen_tcp.connect(host, port, opts, @timeout)

    with {:ok, sock} <- result,
         conn = {if(tls?, do: :ssl, else: :gen_tcp), sock},
         {:ok, "* OK" <> _} <- recv(conn, @timeout) do
      {:ok, conn}
    else
      {:ok, other} -> {:error, {:greeting, other}}
      {:error, reason} -> {:error, {:connect, reason}}
    end
  end

  def send({mod, sock}, data), do: mod.send(sock, data)
  def recv({mod, sock}, timeout), do: mod.recv(sock, 0, timeout)
  def close({mod, sock}), do: mod.close(sock)
  def setopts({:gen_tcp, sock}, opts), do: :inet.setopts(sock, opts)
  def setopts({:ssl, sock}, opts), do: :ssl.setopts(sock, opts)

  @doc "Message tags of active mode: {data, closed}."
  def tags({:gen_tcp, _}), do: {:tcp, :tcp_closed}
  def tags({:ssl, _}), do: {:ssl, :ssl_closed}

  @doc "Send a tagged command, read until the tagged reply. Returns {:ok | :no | :bad, line}."
  def command(sock, tag, line) do
    with :ok <- send(sock, [tag, " ", line, "\r\n"]), do: await(sock, tag <> " ")
  end

  defp await(sock, prefix) do
    case recv(sock, @timeout) do
      {:ok, ^prefix <> "OK" <> _ = line} -> {:ok, line}
      {:ok, ^prefix <> "NO" <> _ = line} -> {:no, line}
      {:ok, ^prefix <> "BAD" <> _ = line} -> {:bad, line}
      {:ok, _untagged} -> await(sock, prefix)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Send a tagged command and collect the untagged response lines until the tagged reply.
  Literals in responses (`{n}` + n bytes, e.g. BODY[] in FETCH) are read and dropped.
  Returns `{:ok | :no | :bad, untagged_lines}` or `{:error, reason}`.
  """
  def exec(sock, tag, line) do
    with :ok <- send(sock, [tag, " ", line, "\r\n"]), do: collect(sock, tag <> " ", [])
  end

  defp collect(sock, prefix, acc) do
    case recv(sock, @timeout) do
      {:ok, ^prefix <> "OK" <> _} -> {:ok, Enum.reverse(acc)}
      {:ok, ^prefix <> "NO" <> _} -> {:no, Enum.reverse(acc)}
      {:ok, ^prefix <> "BAD" <> _} -> {:bad, Enum.reverse(acc)}
      {:ok, line} -> with :ok <- skip_literal(sock, line), do: collect(sock, prefix, [line | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp skip_literal(sock, line) do
    case Regex.run(~r/\{(\d+)\}\r\n$/, line) do
      [_, "0"] ->
        :ok

      [_, n] ->
        :ok = setopts(sock, packet: :raw)
        {mod, raw} = sock
        result = mod.recv(raw, String.to_integer(n), @timeout)
        :ok = setopts(sock, packet: :line)
        with {:ok, _bytes} <- result, do: :ok

      nil ->
        :ok
    end
  end

  @doc "Start IDLE: returns :ok once the server sent the continuation."
  def idle(sock, tag) do
    with :ok <- send(sock, [tag, " IDLE\r\n"]), do: await_continuation(sock)
  end

  defp await_continuation(sock) do
    case recv(sock, @timeout) do
      {:ok, "+" <> _} -> :ok
      {:ok, "* " <> _} -> await_continuation(sock)
      {:ok, other} -> {:error, {:idle, other}}
      {:error, reason} -> {:error, reason}
    end
  end
end

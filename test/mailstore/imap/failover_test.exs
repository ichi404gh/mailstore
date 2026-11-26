defmodule Mailstore.Imap.FailoverTest do
  @moduledoc """
  A shard whose primary turns into a replica (a failover, simulated with
  `default_transaction_read_only` and dropped connections): IMAP writes get
  `NO [UNAVAILABLE]`, nothing is written, and the same session writes again once the
  shard is writable.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{Directory, Shards}
  alias Mailstore.Imap.Session

  @db "mailstore_test_shard_failover"

  setup_all do
    url =
      Application.get_env(:mailstore, Mailstore.Repo)[:url]
      |> String.replace(~r{/[^/]+$}, "/" <> @db)

    {:ok, shard} =
      Shards.register("test-failover", url, create: true, migrate: true, pool_size: 2)

    on_exit(fn ->
      Repo.query!("ALTER DATABASE #{@db} RESET default_transaction_read_only")
      Repo.query!("UPDATE tenants SET shard = 1 WHERE shard = $1", [shard])
      Repo.query!("DELETE FROM shards WHERE id = $1", [shard])
      Shards.refresh_local()
    end)

    %{shard: shard}
  end

  setup %{shard: shard} do
    Shards.repo_for(shard).query!(
      "TRUNCATE folders, messages, message_bodies, vanished, deliveries"
    )

    :ok
  end

  defp cmd(s, line) do
    {out, s} = Session.handle_command(line, s)
    {IO.iodata_to_binary(out), s}
  end

  defp append(s, tag, n) do
    raw = "Subject: #{n}\r\nMessage-ID: <#{n}@failover.test>\r\n\r\nbody\r\n"
    cmd(s, "#{tag} APPEND INBOX {#{byte_size(raw)}+}\r\n#{raw}\r\n")
  end

  # Read-only for new sessions, and every open connection to the shard dropped.
  defp read_only(on) do
    setting =
      if on,
        do: "SET default_transaction_read_only = on",
        else: "RESET default_transaction_read_only"

    Repo.query!("ALTER DATABASE #{@db} #{setting}")

    Repo.query!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", [@db])
  end

  defp eventually(fun, tries \\ 50) do
    case fun.() do
      {:ok, result} -> result
      {:retry, _} when tries > 1 -> Process.sleep(100) && eventually(fun, tries - 1)
      {:retry, last} -> flunk("gave up, last: #{inspect(last)}")
    end
  end

  test "writes during a failover get NO [UNAVAILABLE]; the session goes on afterwards",
       %{shard: shard} do
    {:ok, t} = Directory.create_tenant(%{name: "failover", shard: shard})
    {:ok, d} = Directory.create_domain(t, "failover.test")
    {:ok, _} = Directory.create_account(d, "a", %{password: "pw"})

    {"l OK" <> _, s} = cmd(Session.new(), "l LOGIN a@failover.test pw\r\n")
    {out, s} = append(s, "a1", 1)
    assert out =~ ~r/a1 OK \[APPENDUID \d+ 1\]/

    read_only(true)

    s =
      eventually(fn ->
        {out, s} = append(s, "a2", 2)
        if out =~ "a2 NO [UNAVAILABLE]", do: {:ok, s}, else: {:retry, out}
      end)

    # Once reconnected, the connection is to a read-only server: still NO, no crash.
    {out, s} = append(s, "a2b", 2)
    assert out =~ "a2b NO [UNAVAILABLE]"
    log = ExUnit.CaptureLog.capture_log(fn -> {_, _} = append(s, "a2c", 2) end)
    assert log =~ "read_only_sql_transaction"

    {out, s} = cmd(s, "s1 STATUS INBOX (MESSAGES)\r\n")
    assert out =~ "MESSAGES 1"

    read_only(false)

    out =
      eventually(fn ->
        {out, _} = append(s, "a3", 3)
        if out =~ "a3 OK", do: {:ok, out}, else: {:retry, out}
      end)

    # The failed APPENDs wrote nothing: the next UID is 2.
    assert out =~ ~r/a3 OK \[APPENDUID \d+ 2\]/
  end
end

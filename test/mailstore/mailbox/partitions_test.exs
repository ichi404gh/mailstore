defmodule Mailstore.Mailbox.PartitionsTest do
  @moduledoc """
  Retention policies: classes map to policies, each policy's mail lives in its own
  partitions, and expiry drops a policy's old weeks while other mail of those weeks
  stays, with folder counters fixed and selected sessions told.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Mailbox, Shards}
  alias Mailstore.Imap.Session
  alias Mailstore.Mailbox.{Partitions, Retention}

  @short 1
  @standard 0

  defp at(days_ago),
    do: DateTime.new!(Date.add(Date.utc_today(), -days_ago), ~T[12:00:00.000000], "Etc/UTC")

  defp append(a, class, days_ago, flags \\ []) do
    {:ok, [uid]} =
      Mailbox.append(a.id, "INBOX", [new_message(class: class, flags: flags)],
        stored_at: at(days_ago)
      )

    uid
  end

  defp partition_of(repo, table, folder_id, uid) do
    %{rows: [[name]]} =
      repo.query!(
        "SELECT tableoid::regclass::text FROM #{table} WHERE folder_id = $1 AND uid = $2",
        [
          folder_id,
          uid
        ]
      )

    name
  end

  defp week(%Date{} = d) do
    {y, w} = :calendar.iso_week_number(Date.to_erl(d))
    "#{y}w#{String.pad_leading("#{w}", 2, "0")}"
  end

  defp cmd(s, line) do
    {out, s} = Session.handle_command(line <> "\r\n", s)
    {IO.iodata_to_binary(out), s}
  end

  test "the configuration maps classes to policies and is checked" do
    assert Retention.policy_id(:warmup) == @short
    assert Retention.policy_id(:reply) == @standard
    assert Retention.policy_id(:spam_test) == @standard
    assert [%{name: :standard, cold_after_days: 30}] = Retention.cold_policies()

    bad = [
      [policies: [a: [id: 0, keep_days: 10], b: [id: 0, keep_days: 20]], default: :a],
      [policies: [a: [id: 0, keep_days: 10]], default: :missing],
      [policies: [a: [id: 0, keep_days: 10]], classes: [warmup: :nope], default: :a],
      [policies: [a: [id: 0, keep_days: 10]], classes: [junk: :a], default: :a],
      [policies: [a: [id: 0, keep_days: 10, cold_after_days: 10]], default: :a],
      [policies: [a: [id: 0, keep_days: 0]], default: :a]
    ]

    for env <- bad, do: assert_raise(ArgumentError, fn -> Retention.build(env) end)
  end

  test "mail lands in its policy's partitions, and a copy keeps the policy" do
    a = account_fixture("bob@acme.test")
    repo = Shards.repo(a.tenant_id)
    {:ok, [_]} = Delivery.deliver(raw_message(), a.address, class: :warmup)
    {:ok, [_]} = Delivery.deliver(raw_message(), a.address, class: :reply)
    {:ok, inbox} = Mailbox.status(a.id, "INBOX")
    {:ok, junk} = Mailbox.status(a.id, "Junk")

    week = week(Date.utc_today())

    assert partition_of(repo, "messages", inbox.id, 1) == "messages_p1_#{week}"
    assert partition_of(repo, "message_bodies", inbox.id, 1) == "message_bodies_p1_#{week}"
    assert partition_of(repo, "messages", inbox.id, 2) == "messages_p0_#{week}"

    {:ok, _} = Mailbox.copy(a.id, "INBOX", [1], "Junk")
    assert partition_of(repo, "messages", junk.id, 1) == "messages_p1_#{week}"
    assert partition_of(repo, "message_bodies", junk.id, 1) == "message_bodies_p1_#{week}"
  end

  test "expiry drops each policy's old weeks, fixes the counters and tells the sessions" do
    a = account_fixture("bob@acme.test", password: "pw")
    repo = Shards.repo(a.tenant_id)
    Partitions.ensure(repo, Date.add(Date.utc_today(), -400), 62)

    gone_standard = append(a, :reply, 400)
    gone_short_seen = append(a, :warmup, 40, ["\\Seen"])
    gone_short = append(a, :warmup, 40)
    kept_standard = append(a, :reply, 40)
    kept_short = append(a, :warmup, 0)
    kept_new = append(a, :reply, 0, ["\\Seen"])
    assert [gone_standard, gone_short_seen, gone_short] == [1, 2, 3]

    {:ok, before} = Mailbox.status(a.id, "INBOX")
    assert before.messages_count == 6 and before.unseen_count == 4

    {"l OK" <> _, s} = cmd(Session.new(), "l LOGIN bob@acme.test pw")
    {out, s} = cmd(s, "s SELECT INBOX")
    assert out =~ "s OK"

    # The folder had mail in an expired week of each policy.
    assert {:ok, %{weeks: weeks, folder_updates: 2}} = Mailbox.apply_retention()
    assert Enum.any?(weeks, &String.starts_with?(&1, "messages_p1_"))
    assert Enum.any?(weeks, &String.starts_with?(&1, "messages_p0_"))

    assert Mailbox.uids(a.tenant_id, before.id) == [kept_standard, kept_short, kept_new]

    # Bodies of the dropped weeks are gone too; the kept ones are readable.
    assert repo.query!("SELECT uid FROM message_bodies WHERE folder_id = $1 ORDER BY uid", [
             before.id
           ]).rows ==
             [[kept_standard], [kept_short], [kept_new]]

    {:ok, now} = Mailbox.status(a.id, "INBOX")
    assert now.messages_count == 3 and now.unseen_count == 2
    assert now.highest_modseq > before.highest_modseq

    # The selected session learns that UIDs 1..3 went.
    assert_receive {:mailstore, _, {:reload, _}} = push, 2_000
    {_, s} = Session.handle_push(push, s)
    {out, _} = cmd(s, "n NOOP")
    assert out == "* 3 EXPUNGE\r\n* 2 EXPUNGE\r\n* 1 EXPUNGE\r\nn OK NOOP completed\r\n"

    # Nothing left to expire.
    assert {:ok, %{weeks: [], folder_updates: 0}} = Mailbox.apply_retention()
  end

  test "making a partition behind a long query gives up instead of queueing every write" do
    a = account_fixture("bob@acme.test")
    repo = Shards.repo(a.tenant_id)
    week = ~D[2031-01-06]
    parent = self()

    # A long reader of messages (a big SEARCH, pg_dump).
    reader =
      spawn_link(fn ->
        repo.transaction(fn ->
          repo.query!("SELECT count(*) FROM messages")
          send(parent, :reading)
          receive do: (:done -> :ok)
        end)

        send(parent, :read)
      end)

    assert_receive :reading
    {us, result} = :timer.tc(fn -> catch_error(Partitions.ensure(repo, week, 1)) end)
    assert %Postgrex.Error{postgres: %{code: :lock_not_available}} = result
    assert us < 5_000_000

    # Writes waited at most those 2 s and go through now; once the reader is gone it works.
    {:ok, [_]} = Mailbox.append(a.id, "INBOX", [new_message()])
    send(reader, :done)
    assert_receive :read
    assert :ok = Partitions.ensure(repo, week, 1)

    for t <- ~w(messages message_bodies cold_bodies),
        id <- Retention.ids(),
        do: repo.query!("DROP TABLE IF EXISTS #{t}_p#{id}_2031w02")
  end

  test "partitions that exist are ensured without a lock, behind a long query too" do
    a = account_fixture("bob@acme.test")
    repo = Shards.repo(a.tenant_id)
    parent = self()

    reader =
      spawn_link(fn ->
        repo.transaction(fn ->
          repo.query!("SELECT count(*) FROM messages")
          send(parent, :reading)
          receive do: (:done -> :ok)
        end)
      end)

    assert_receive :reading
    {us, :ok} = :timer.tc(fn -> Partitions.ensure(repo) end)
    assert us < 1_000_000
    send(reader, :done)
  end

  test "a run that died after detaching a week is finished by the next one" do
    a = account_fixture("bob@acme.test")
    repo = Shards.repo(a.tenant_id)
    Partitions.ensure(repo, Date.add(Date.utc_today(), -50), 10)
    append(a, :warmup, 40)
    append(a, :warmup, 40)
    append(a, :reply, 0)

    cutoff = Date.add(Date.utc_today(), -30)
    name = "messages_p1_" <> week(Date.add(Date.utc_today(), -40))
    assert {name, true} in Partitions.expired(repo, @short, cutoff)
    repo.query!("ALTER TABLE messages_p1 DETACH PARTITION #{name}")
    assert {name, false} in Partitions.expired(repo, @short, cutoff)

    assert {:ok, %{weeks: weeks, folder_updates: 1}} = Mailbox.apply_retention()
    assert name in weeks
    {:ok, inbox} = Mailbox.status(a.id, "INBOX")
    assert inbox.messages_count == 1 and inbox.unseen_count == 1

    assert Partitions.expired(repo, @short, Date.add(Date.utc_today(), -30), "message_bodies") ==
             []
  end
end

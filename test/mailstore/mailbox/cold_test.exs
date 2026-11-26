defmodule Mailstore.Mailbox.ColdTest do
  @moduledoc """
  The cold tier with a local directory as the store: a due week's bodies move into
  packs and read back byte for byte, expunge and copy keep working, expiry deletes the
  packs, and a run that died mid-upload leaves nothing behind.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{ColdStore, Mailbox, Shards}
  alias Mailstore.Imap.Session
  alias Mailstore.Mailbox.{Cold, Partitions}

  @root Application.compile_env(:mailstore, :cold_store)[:root]

  setup do
    File.rm_rf!(@root)
    a = account_fixture("bob@acme.test", password: "pw")
    repo = Shards.repo(a.tenant_id)
    Partitions.ensure(repo, Date.add(Date.utc_today(), -60), 12)
    %{a: a, repo: repo}
  end

  defp at(days_ago),
    do: DateTime.new!(Date.add(Date.utc_today(), -days_ago), ~T[12:00:00.000000], "Etc/UTC")

  # Distinct, poorly compressible text, so packs fill up.
  defp raw(n, size) do
    body =
      :crypto.strong_rand_bytes(size)
      |> Base.encode64()
      |> String.graphemes()
      |> Enum.chunk_every(76)
      |> Enum.map_join("\r\n", &Enum.join/1)

    "From: s@example.org\r\nTo: bob@acme.test\r\nSubject: old #{n}\r\n" <>
      "Message-ID: <old-#{n}@example.org>\r\n\r\n#{body}\r\n"
  end

  defp append(a, raw, class, days_ago) do
    msg = Mailstore.Core.NewMessage.from_raw(raw, class: class)
    {:ok, [uid]} = Mailbox.append(a.id, "INBOX", [msg], stored_at: at(days_ago))
    uid
  end

  defp week(%Date{} = d) do
    {y, w} = :calendar.iso_week_number(Date.to_erl(d))
    "#{y}w#{String.pad_leading("#{w}", 2, "0")}"
  end

  defp table?(repo, name),
    do: repo.query!("SELECT to_regclass($1) IS NOT NULL", [name]).rows == [[true]]

  test "a due week moves to the store and reads back byte for byte", %{a: a, repo: repo} do
    raws =
      for n <- 1..6,
          into: %{},
          do:
            (
              r = raw(n, 20_000 + n * 3_000)
              {append(a, r, :reply, 40), r}
            )

    warmup = append(a, raw(7, 1_000), :warmup, 40)
    fresh = append(a, raw(8, 1_000), :reply, 0)
    old_week = week(Date.add(Date.utc_today(), -40))

    assert {:ok, %{weeks: weeks, packs: packs, bodies: 6}} = Cold.run()
    assert "message_bodies_p0_#{old_week}" in weeks
    # 64 KB packs (config/test.exs): six bodies of 27-44 KB need several.
    assert packs >= 3

    refute table?(repo, "message_bodies_p0_#{old_week}")
    assert table?(repo, "message_bodies_p1_#{old_week}"), "warmup has no cold tier"
    {:ok, inbox} = Mailbox.status(a.id, "INBOX")

    assert repo.query!("SELECT count(*) FROM message_bodies WHERE folder_id = $1", [inbox.id]).rows ==
             [[2]]

    uids = Map.keys(raws) ++ [warmup, fresh]
    bodies = Mailbox.fetch_raw_many(a.tenant_id, inbox.id, uids)

    for {uid, raw} <- raws, do: assert(bodies[uid] == raw, "body #{uid} differs")
    assert map_size(bodies) == 8
    [first | _] = Enum.sort(Map.keys(raws))
    assert Mailbox.fetch_raw(a.tenant_id, inbox.id, first) == {:ok, raws[first]}

    # Through IMAP.
    {_, s} = Session.handle_command("l LOGIN bob@acme.test pw\r\n", Session.new())
    {_, s} = Session.handle_command("s SELECT INBOX\r\n", s)
    {out, _} = Session.handle_command("f UID FETCH #{first} BODY.PEEK[]\r\n", s)
    assert IO.iodata_to_binary(out) =~ raws[first]

    # A pack is readable without the database: its index matches the locations.
    %{rows: [[key, pack_id]]} =
      repo.query!("SELECT key, id FROM cold_packs ORDER BY id LIMIT 1")

    pack = File.read!(Path.join(@root, key))

    index =
      repo.query!(
        ~s{SELECT folder_id, uid, "offset", length, codec FROM cold_bodies WHERE pack_id = $1 ORDER BY uid},
        [pack_id]
      ).rows
      |> Enum.map(&List.to_tuple/1)

    assert Enum.sort(Cold.decode_index(pack)) == index

    assert {:ok, %{weeks: [], packs: 0}} = Cold.run()
  end

  test "expunge and copy work on cold mail; expiry deletes the packs", %{a: a, repo: repo} do
    raws =
      for n <- 1..4,
          into: %{},
          do:
            (
              r = raw(n, 30_000)
              {append(a, r, :reply, 40), r}
            )

    [u1, u2, u3, u4] = raws |> Map.keys() |> Enum.sort()
    {:ok, %{packs: packs}} = Cold.run()
    {:ok, inbox} = Mailbox.status(a.id, "INBOX")

    # EXPUNGE removes the location (the bytes stay in the pack until expiry).
    {:ok, _} = Mailbox.store_flags(a.id, "INBOX", [u1], :add, ["\\Deleted"])
    {:ok, [^u1]} = Mailbox.expunge(a.id, "INBOX")

    assert repo.query!("SELECT count(*) FROM cold_bodies WHERE folder_id = $1", [inbox.id]).rows ==
             [[3]]

    assert Mailbox.fetch_raw(a.tenant_id, inbox.id, u2) == {:ok, raws[u2]}

    # COPY brings the body back to Postgres, in the current week.
    {:ok, junk} = Mailbox.status(a.id, "Junk")
    {:ok, _} = Mailbox.copy(a.id, "INBOX", [u2, u3], "Junk")

    assert repo.query!(
             "SELECT uid, tableoid::regclass::text FROM message_bodies WHERE folder_id = $1 ORDER BY uid",
             [junk.id]
           ).rows ==
             [
               [1, "message_bodies_p0_#{week(Date.utc_today())}"],
               [2, "message_bodies_p0_#{week(Date.utc_today())}"]
             ]

    # The old week expires: locations and packs go; the copies stay readable.
    assert length(File.ls!(@root)) > 0
    %{weeks: [_ | _]} = Partitions.expire(repo, 0, Date.add(Date.utc_today(), -20))
    assert repo.query!("SELECT count(*) FROM cold_packs").rows == [[0]]
    assert repo.query!("SELECT count(*) FROM cold_bodies").rows == [[0]]
    assert Path.wildcard(Path.join(@root, "**/*.pack")) == []
    assert packs > 0

    assert Mailbox.fetch_raw_many(a.tenant_id, junk.id, [1, 2]) == %{1 => raws[u2], 2 => raws[u3]}
    assert Mailbox.fetch_raw(a.tenant_id, inbox.id, u4) == :error
  end

  test "a run that died mid-upload is cleaned up by the next one", %{a: a, repo: repo} do
    raw = raw(1, 5_000)
    uid = append(a, raw, :reply, 40)

    key = "t#{a.tenant_id}/p0/#{week(Date.add(Date.utc_today(), -40))}/crashed.pack"
    :ok = ColdStore.put(key, "half a pack")

    repo.query!(
      "INSERT INTO cold_packs (tenant_id, retention, week, key, bytes) VALUES ($1, 0, $2, $3, 11)",
      [a.tenant_id, Date.add(Date.utc_today(), -40), key]
    )

    assert {:ok, %{packs: 1, bodies: 1}} = Cold.run()
    assert ColdStore.size(key) == {:error, :not_found}
    assert repo.query!("SELECT count(*) FROM cold_packs WHERE state = 'stored'").rows == [[1]]
    assert repo.query!("SELECT count(*) FROM cold_packs").rows == [[1]]
    {:ok, inbox} = Mailbox.status(a.id, "INBOX")
    assert Mailbox.fetch_raw(a.tenant_id, inbox.id, uid) == {:ok, raw}
  end

  test "weeks of policies without a cold tier, and weeks not yet due, stay", %{a: a, repo: repo} do
    append(a, raw(1, 1_000), :warmup, 40)
    append(a, raw(2, 1_000), :reply, 10)
    # (Empty old weeks may be dropped; nothing is packed.)
    assert {:ok, %{packs: 0, bodies: 0}} = Cold.run()
    assert repo.query!("SELECT count(*) FROM message_bodies").rows == [[2]]
  end
end

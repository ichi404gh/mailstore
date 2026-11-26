defmodule Mailstore.Shard.Migrations.PartitionByRetention do
  @moduledoc """
  `messages` and `message_bodies` partitioned by retention policy, then by week
  (`Mailstore.Mailbox.Retention`): a policy's old weeks are dropped whole while
  other policies' messages of the same week stay.

  A partitioned table cannot change its partition key, so both tables are recreated
  and the rows copied, each getting the policy of its class under the configuration
  at the time of the migration.
  """
  use Ecto.Migration

  @indexes ~w(messages_pkey messages_folder_modseq messages_folder_unseen messages_tenant_msgid
              messages_tenant_in_reply_to messages_tenant_thread message_bodies_pkey)

  def up do
    retention = Mailstore.Mailbox.Retention.load!()
    ids = Enum.map_join(retention.policies, ",", & &1.id)

    by_class =
      retention.by_class
      |> Tuple.to_list()
      |> Enum.with_index()
      |> Enum.map_join(" ", fn {id, class} -> "WHEN #{class} THEN #{id}" end)

    execute "ALTER TABLE messages RENAME TO messages_legacy"
    execute "ALTER TABLE message_bodies RENAME TO message_bodies_legacy"

    for index <- @indexes,
        do: execute("ALTER INDEX #{index} RENAME TO legacy_#{index}")

    execute """
    CREATE TABLE messages (
      tenant_id      bigint      NOT NULL,
      folder_id      bigint      NOT NULL,
      uid            bigint      NOT NULL,
      modseq         bigint      NOT NULL,
      stored_at      timestamptz NOT NULL,
      retention      smallint    NOT NULL,
      internal_date  timestamptz NOT NULL,
      size           integer     NOT NULL,
      flags          smallint    NOT NULL DEFAULT 0,
      keywords       text[]      NOT NULL DEFAULT '{}',
      class          smallint    NOT NULL DEFAULT 0,
      message_id     text,
      message_id_h   bigint,
      in_reply_to_h  bigint,
      thread_id      bigint,
      from_addr      text,
      subject        text,
      envelope       jsonb,
      bodystructure  jsonb,
      PRIMARY KEY (folder_id, uid, stored_at, retention)
    ) PARTITION BY LIST (retention)
    """

    execute "CREATE INDEX messages_folder_modseq ON messages (folder_id, modseq)"

    execute "CREATE INDEX messages_folder_unseen ON messages (folder_id, uid) WHERE (flags & 1) = 0"

    execute "CREATE INDEX messages_tenant_msgid ON messages (tenant_id, message_id_h)"

    execute """
    CREATE INDEX messages_tenant_in_reply_to ON messages (tenant_id, in_reply_to_h)
    WHERE in_reply_to_h IS NOT NULL
    """

    execute """
    CREATE INDEX messages_tenant_thread ON messages (tenant_id, thread_id)
    WHERE thread_id IS NOT NULL
    """

    execute """
    CREATE TABLE message_bodies (
      folder_id  bigint      NOT NULL,
      uid        bigint      NOT NULL,
      stored_at  timestamptz NOT NULL,
      retention  smallint    NOT NULL,
      codec      smallint    NOT NULL,
      data       bytea       NOT NULL,
      PRIMARY KEY (folder_id, uid, stored_at, retention)
    ) PARTITION BY LIST (retention)
    """

    execute "ALTER TABLE message_bodies ALTER COLUMN data SET STORAGE EXTERNAL"

    execute "DROP FUNCTION mailstore_ensure_partitions(date, int)"

    # Per policy a list partition, inside it weekly range partitions:
    # messages_p<id>_<IYYY>w<IW>, message_bodies_p<id>_<IYYY>w<IW>.
    execute """
    CREATE FUNCTION mailstore_ensure_partitions(p_policies smallint[], p_from date, p_weeks int)
    RETURNS void LANGUAGE plpgsql AS $$
    DECLARE
      p smallint;
      t text;
      w date;
    BEGIN
      FOREACH p IN ARRAY p_policies LOOP
        FOREACH t IN ARRAY ARRAY['messages', 'message_bodies'] LOOP
          EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES IN (%s) PARTITION BY RANGE (stored_at)',
            t || '_p' || p, t, p);
          w := date_trunc('week', p_from)::date;
          FOR i IN 1..p_weeks LOOP
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
              t || '_p' || p || '_' || to_char(w, 'IYYY"w"IW'), t || '_p' || p,
              w::timestamp AT TIME ZONE 'UTC', (w + 7)::timestamp AT TIME ZONE 'UTC');
            w := w + 7;
          END LOOP;
        END LOOP;
      END LOOP;
    END $$
    """

    # Partitions for every week that has rows, up to a few weeks ahead.
    execute """
    DO $$
    DECLARE f date;
    BEGIN
      SELECT LEAST(COALESCE(min(stored_at)::date, now()::date), (now() - interval '7 days')::date)
        INTO f FROM messages_legacy;
      PERFORM mailstore_ensure_partitions(ARRAY[#{ids}]::smallint[], f, ((now()::date - f) / 7 + 6)::int);
    END $$
    """

    execute """
    INSERT INTO messages (tenant_id, folder_id, uid, modseq, stored_at, retention, internal_date,
      size, flags, keywords, class, message_id, message_id_h, in_reply_to_h, thread_id,
      from_addr, subject, envelope, bodystructure)
    SELECT tenant_id, folder_id, uid, modseq, stored_at, CASE class #{by_class} END, internal_date,
      size, flags, keywords, class, message_id, message_id_h, in_reply_to_h, thread_id,
      from_addr, subject, envelope, bodystructure
    FROM messages_legacy
    """

    execute """
    INSERT INTO message_bodies (folder_id, uid, stored_at, retention, codec, data)
    SELECT b.folder_id, b.uid, b.stored_at, m.retention, b.codec, b.data
    FROM message_bodies_legacy b
    JOIN messages m ON m.folder_id = b.folder_id AND m.uid = b.uid AND m.stored_at = b.stored_at
    """

    execute "DROP TABLE message_bodies_legacy"
    execute "DROP TABLE messages_legacy"
  end

  def down do
    raise Ecto.MigrationError, "partitioning by retention policy cannot be undone"
  end
end

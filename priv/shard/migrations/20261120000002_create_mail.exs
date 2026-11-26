defmodule Mailstore.Shard.Migrations.CreateMail do
  @moduledoc """
  Mail shard: folders, message metadata, bodies, expunge log.

  No foreign keys to the directory on purpose: in production the directory and the
  shards are different databases.
  """
  use Ecto.Migration

  def up do
    create table(:folders) do
      add :tenant_id, :bigint, null: false
      add :account_id, :bigint, null: false
      add :name, :text, null: false
      add :special_use, :text
      # UID namespace of the folder. UIDVALIDITY must survive migration from Dovecot.
      add :uidvalidity, :bigint, null: false
      add :uidnext, :bigint, null: false, default: 1
      add :highest_modseq, :bigint, null: false, default: 1
      # Retention drops whole partitions; everything below this UID is gone (VANISHED 1:n).
      add :retained_from_uid, :bigint, null: false, default: 1
      # Kept in the same UPDATE that allocates UIDs, so STATUS never scans messages.
      add :messages_count, :integer, null: false, default: 0
      add :unseen_count, :integer, null: false, default: 0
      add :subscribed, :boolean, null: false, default: true
      timestamps()
    end

    create unique_index(:folders, [:account_id, :name])

    # Narrow metadata table: FLAGS/UID/SEARCH scans never read bodies.
    # Partitioned by server storage time so retention is DROP TABLE, not DELETE.
    execute """
    CREATE TABLE messages (
      tenant_id      bigint      NOT NULL,
      folder_id      bigint      NOT NULL,
      uid            bigint      NOT NULL,
      modseq         bigint      NOT NULL,
      stored_at      timestamptz NOT NULL,
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
      PRIMARY KEY (folder_id, uid, stored_at)
    ) PARTITION BY RANGE (stored_at)
    """

    # CONDSTORE / QRESYNC: what changed since modseq X
    execute "CREATE INDEX messages_folder_modseq ON messages (folder_id, modseq)"
    # UNSEEN is the poller's main query; \Seen = bit 1
    execute "CREATE INDEX messages_folder_unseen ON messages (folder_id, uid) WHERE (flags & 1) = 0"
    # Threading and SEARCH HEADER Message-ID
    execute "CREATE INDEX messages_tenant_msgid ON messages (tenant_id, message_id_h)"

    execute """
    CREATE INDEX messages_tenant_in_reply_to ON messages (tenant_id, in_reply_to_h)
    WHERE in_reply_to_h IS NOT NULL
    """

    execute """
    CREATE INDEX messages_tenant_thread ON messages (tenant_id, thread_id)
    WHERE thread_id IS NOT NULL
    """

    # Raw RFC 5322 bytes, zstd-compressed by the application (messages are ~2 KB,
    # below the TOAST threshold, so Postgres would not compress them itself).
    execute """
    CREATE TABLE message_bodies (
      folder_id  bigint      NOT NULL,
      uid        bigint      NOT NULL,
      stored_at  timestamptz NOT NULL,
      codec      smallint    NOT NULL,
      data       bytea       NOT NULL,
      PRIMARY KEY (folder_id, uid, stored_at)
    ) PARTITION BY RANGE (stored_at)
    """

    # Already compressed: do not let TOAST try again on the rare large message.
    execute "ALTER TABLE message_bodies ALTER COLUMN data SET STORAGE EXTERNAL"

    # Expunged UIDs for QRESYNC VANISHED. Trimmed by a periodic job.
    execute """
    CREATE TABLE vanished (
      folder_id  bigint NOT NULL,
      uid        bigint NOT NULL,
      modseq     bigint NOT NULL,
      PRIMARY KEY (folder_id, uid)
    )
    """

    execute "CREATE INDEX vanished_folder_modseq ON vanished (folder_id, modseq)"

    # Weekly partitions for both partitioned tables, named <table>_<IYYY>w<IW>.
    execute """
    CREATE OR REPLACE FUNCTION mailstore_ensure_partitions(p_from date, p_weeks int)
    RETURNS void LANGUAGE plpgsql AS $$
    DECLARE
      w date := date_trunc('week', p_from)::date;
      t text;
    BEGIN
      FOR i IN 1..p_weeks LOOP
        FOREACH t IN ARRAY ARRAY['messages', 'message_bodies'] LOOP
          EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            t || '_' || to_char(w, 'IYYY"w"IW'), t,
            w::timestamp AT TIME ZONE 'UTC', (w + 7)::timestamp AT TIME ZONE 'UTC');
        END LOOP;
        w := w + 7;
      END LOOP;
    END $$
    """

    execute "SELECT mailstore_ensure_partitions((now() - interval '7 days')::date, 12)"
  end

  def down do
    execute "DROP FUNCTION IF EXISTS mailstore_ensure_partitions(date, int)"
    execute "DROP TABLE IF EXISTS vanished"
    execute "DROP TABLE IF EXISTS message_bodies"
    execute "DROP TABLE IF EXISTS messages"
    drop table(:folders)
  end
end

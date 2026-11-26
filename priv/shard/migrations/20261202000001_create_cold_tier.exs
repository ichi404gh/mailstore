defmodule Mailstore.Shard.Migrations.CreateColdTier do
  @moduledoc """
  The cold tier (`Mailstore.Mailbox.Cold`): bodies of old weeks packed into objects of
  the cold store, one pack per tenant, policy and week (split by size).

  * `cold_packs`: the objects. Ids come from the shard's range like folder ids
    (`Mailstore.Shards`), so a moving tenant takes its rows along unchanged.
    `state`: `uploading` until the store confirmed the object, then `stored`.
  * `cold_bodies`: where each body is, partitioned like `message_bodies` (policy, then
    week), so a week's locations are dropped whole when the week expires.
  """
  use Ecto.Migration

  def up do
    execute """
    CREATE TABLE cold_packs (
      id          bigserial   PRIMARY KEY,
      tenant_id   bigint      NOT NULL,
      retention   smallint    NOT NULL,
      week        date        NOT NULL,
      key         text        NOT NULL UNIQUE,
      bytes       bigint      NOT NULL,
      state       text        NOT NULL DEFAULT 'uploading',
      inserted_at timestamptz NOT NULL DEFAULT now(),
      CHECK (state IN ('uploading', 'stored'))
    )
    """

    execute "CREATE INDEX cold_packs_week ON cold_packs (retention, week)"
    execute "CREATE INDEX cold_packs_tenant ON cold_packs (tenant_id)"

    execute """
    CREATE TABLE cold_bodies (
      folder_id  bigint      NOT NULL,
      uid        bigint      NOT NULL,
      stored_at  timestamptz NOT NULL,
      retention  smallint    NOT NULL,
      pack_id    bigint      NOT NULL,
      "offset"   bigint      NOT NULL,
      length     integer     NOT NULL,
      codec      smallint    NOT NULL,
      PRIMARY KEY (folder_id, uid, stored_at, retention)
    ) PARTITION BY LIST (retention)
    """

    # Same partitions for cold_bodies as for messages and message_bodies.
    execute """
    CREATE OR REPLACE FUNCTION mailstore_ensure_partitions(p_policies smallint[], p_from date, p_weeks int)
    RETURNS void LANGUAGE plpgsql AS $$
    DECLARE
      p smallint;
      t text;
      w date;
    BEGIN
      FOREACH p IN ARRAY p_policies LOOP
        FOREACH t IN ARRAY ARRAY['messages', 'message_bodies', 'cold_bodies'] LOOP
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

    # cold_bodies weeks for every week messages already has.
    execute """
    DO $$
    DECLARE r record;
    BEGIN
      FOR r IN
        SELECT (regexp_match(c.relname, '^messages_p(\\d+)_(\\d{4}w\\d{2})$')) AS m
        FROM pg_class c WHERE c.relname ~ '^messages_p\\d+_\\d{4}w\\d{2}$'
      LOOP
        PERFORM mailstore_ensure_partitions(ARRAY[r.m[1]::smallint], to_date(r.m[2], 'IYYY"w"IW'), 1);
      END LOOP;
    END $$
    """
  end

  def down do
    execute "DROP TABLE cold_bodies"
    execute "DROP TABLE cold_packs"
  end
end

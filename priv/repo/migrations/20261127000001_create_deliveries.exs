defmodule Mailstore.Repo.Migrations.CreateDeliveries do
  @moduledoc """
  Deliveries already stored, for idempotent ingest: a message replayed from Kafka (a
  restart before the offset commit, a DLQ replay, a producer retry) is recognized by
  its key and not stored twice.

  Partitioned by day of insertion, so the dedup horizon is `DROP TABLE` of old days.
  The insertion time is not deterministic, so the primary key alone does not catch a
  replay that lands on another day: the insert checks all partitions first
  (`Commands.deliver/2`), the key only closes the race between concurrent
  transactions on the same day.
  """
  use Ecto.Migration

  def up do
    execute """
    CREATE TABLE deliveries (
      account_id  bigint      NOT NULL,
      key         bytea       NOT NULL,
      created_at  timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (account_id, key, created_at)
    ) PARTITION BY RANGE (created_at)
    """

    # Daily partitions named deliveries_<YYYYMMDD>.
    execute """
    CREATE OR REPLACE FUNCTION mailstore_ensure_delivery_partitions(p_from date, p_days int)
    RETURNS void LANGUAGE plpgsql AS $$
    DECLARE
      d date := p_from;
    BEGIN
      FOR i IN 1..p_days LOOP
        EXECUTE format(
          'CREATE TABLE IF NOT EXISTS %I PARTITION OF deliveries FOR VALUES FROM (%L) TO (%L)',
          'deliveries_' || to_char(d, 'YYYYMMDD'),
          d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
        d := d + 1;
      END LOOP;
    END $$
    """

    execute "SELECT mailstore_ensure_delivery_partitions((now() - interval '1 day')::date, 8)"
  end

  def down do
    execute "DROP FUNCTION IF EXISTS mailstore_ensure_delivery_partitions(date, int)"
    execute "DROP TABLE IF EXISTS deliveries"
  end
end

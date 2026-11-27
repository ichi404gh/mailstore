import Config

config :mailstore, ecto_repos: [Mailstore.Repo]

config :mailstore, Mailstore.Repo,
  migration_timestamps: [type: :utc_datetime_usec],
  # Postgrex sets its socket `buffer` (user-space read buffer) to max(recbuf, sndbuf):
  # ~400 KB with OS defaults, allocated for every reply however small. Our replies are
  # a few KB. Capped also because the inet driver caches freed read buffers per
  # scheduler and hands any big-enough one to the next socket that asks.
  socket_options: [buffer: 65_536],
  # A failover: the old primary is a replica now (see Mailstore.Shards.repo_config/1).
  disconnect_on_error_codes: [:read_only_sql_transaction]

# How long an account actor lives without subscribers. Longer than the sequencers' poll
# interval, so polling does not restart the actor every time.
config :mailstore, :account_idle_timeout, :timer.minutes(10)

# IMAP listener. TLS is terminated in front (HAProxy), so this is plain TCP.
config :mailstore, :imap,
  enabled: true,
  # direct connections
  port: 1143,
  # HAProxy (TLS on 993) connects here with a PROXY protocol header
  proxy_port: 1144,
  num_acceptors: 16,
  hibernate_after: 15_000

# Directory in ETS: poll for changed rows, periodic full reload (drops hard deletes),
# how long a successful LOGIN is remembered.
config :mailstore, :directory_cache,
  poll_ms: 1_000,
  full_reload_ms: :timer.minutes(10),
  auth_ttl_ms: :timer.hours(1)

# Retention (Mailstore.Mailbox.Retention): how long mail is kept per policy, and when
# its bodies may go to the cold tier; message classes map to policies. A week is dropped
# once it ended more than keep_days ago. Never reuse a policy id for another policy.
# MAILSTORE_RETENTION (JSON, see config/runtime.exs) overrides this at runtime.
config :mailstore, :retention,
  policies: [
    standard: [id: 0, keep_days: 365, cold_after_days: 30],
    # Incoming warmup is not read after a couple of days.
    short: [id: 1, keep_days: 30]
  ],
  classes: [warmup: :short],
  default: :standard,
  # Run expiry and the cold tier from Mailbox.PartitionMaintainer (every 12 h).
  schedule: true

# Cold tier store (Mailstore.ColdStore). Not set: bodies stay in Postgres until they
# expire. S3 API (MinIO, AWS S3, SeaweedFS...) or a local directory; pack_bytes splits
# a tenant's week into objects of at most that size. Environment variables in
# config/runtime.exs.
#
#   config :mailstore, :cold_store,
#     adapter: Mailstore.ColdStore.S3,
#     endpoint: "http://localhost:8333", bucket: "mailstore-cold", region: "us-east-1",
#     access_key_id: "...", secret_access_key: "...", prefix: "cold",
#     pack_bytes: 64 * 1024 * 1024
#
#   config :mailstore, :cold_store, adapter: Mailstore.ColdStore.Local, root: "/var/lib/mailstore/cold"

# Inbound queue: Kafka topics filled by the SMTP layer (see Mailstore.Ingest). Off by
# default; MAILSTORE_KAFKA_HOSTS turns it on (config/runtime.exs).
config :mailstore, :ingest,
  enabled: false,
  hosts: [localhost: 9092],
  # E.g. a normal and a priority lane: both are consumed by one pipeline.
  topics: ["mailstore.inbound", "mailstore.inbound.priority"],
  group_id: "mailstore-ingest",
  # Where a new consumer group starts. Replays are harmless (idempotent writes).
  offset_reset_policy: :latest,
  offset_commit_interval_seconds: 5,
  dead_letter_topic: nil,
  # One batch = one transaction per shard. At 1 000 records/s a 200 ms window gives
  # batches of ~100 and a lag of ~0.3 s (bench/README.md, "Очередь входящих").
  batch_size: 200,
  batch_timeout: 200,
  batchers: 2

# Outbound: forwards to external targets (see Mailstore.Outbound). Off by default.
# Adapter Mailstore.Outbound.Kafka writes them to the relay sender's topic; the
# MAILSTORE_OUTBOUND_KAFKA_* variables turn it on (config/runtime.exs). `relay` is the
# relay sender's name for the SMTP relay to send through (its `server_name`); the
# topic's max.message.bytes must allow `max_record_bytes`.
config :mailstore, :outbound,
  enabled: false,
  adapter: Mailstore.Outbound.Kafka,
  hosts: [localhost: 9092],
  topic: "mailstore.outbound",
  relay: nil,
  max_record_bytes: 16 * 1024 * 1024,
  ack_timeout: 30_000

# How long delivered keys are remembered (dedup of replays). Must exceed the retention
# of anything ingest can replay from: the source topics and the dead-letter topic.
config :mailstore, :delivery_dedup_days, 8

# PBKDF2 cost for new password hashes.
config :mailstore, :pbkdf2_iterations, 100_000

import_config "#{config_env()}.exs"

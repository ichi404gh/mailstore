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

# How long delivered keys are remembered (dedup of replays). Must exceed the retention
# of anything ingest can replay from: the source topics and the dead-letter topic.
config :mailstore, :delivery_dedup_days, 8

# PBKDF2 cost for new password hashes.
config :mailstore, :pbkdf2_iterations, 100_000

import_config "#{config_env()}.exs"

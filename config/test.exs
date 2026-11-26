import Config

config :mailstore, Mailstore.Repo,
  url:
    System.get_env("TEST_DATABASE_URL", "ecto://postgres:postgres@localhost:55432/mailstore_test"),
  pool_size: 20

# Directory: mailstore_test; mail: mailstore_test_shard1 (created by `mix test`).
config :mailstore, :bootstrap_shards, [
  %{
    name: "shard1",
    url:
      System.get_env(
        "TEST_SHARD1_URL",
        "ecto://postgres:postgres@localhost:55432/mailstore_test_shard1"
      ),
    create: true,
    pool_size: 20
  }
]

config :logger, level: :warning

# Random free port; tests read it with Mailstore.Imap.port/0.
config :mailstore, :imap, enabled: true, port: 0, proxy_port: 0, num_acceptors: 4

config :mailstore, :pbkdf2_iterations, 1_000

# Tests run retention themselves (Mailbox.apply_retention/1) with their own dates.
config :mailstore, :retention, schedule: false

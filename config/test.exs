import Config

config :mailstore, Mailstore.Repo,
  url:
    System.get_env("TEST_DATABASE_URL", "ecto://postgres:postgres@localhost:55432/mailstore_test"),
  pool_size: 20

config :logger, level: :warning

# Random free port; tests read it with Mailstore.Imap.port/0.
config :mailstore, :imap, enabled: true, port: 0, proxy_port: 0, num_acceptors: 4

config :mailstore, :pbkdf2_iterations, 1_000

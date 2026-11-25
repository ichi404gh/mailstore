import Config

config :mailstore, Mailstore.Repo,
  url: System.get_env("DATABASE_URL", "ecto://postgres:postgres@localhost:55432/mailstore_dev"),
  pool_size: 20

config :logger, level: :info

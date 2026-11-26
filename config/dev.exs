import Config

config :mailstore, Mailstore.Repo,
  url: System.get_env("DATABASE_URL", "ecto://postgres:postgres@localhost:55432/mailstore_dev"),
  pool_size: 20

# The first shard (mail) lives in a database of its own; the directory is mailstore_dev.
# `mix mailstore.migrate` creates and registers it.
config :mailstore, :bootstrap_shards, [
  %{
    name: "shard1",
    url:
      System.get_env(
        "SHARD1_URL",
        "ecto://postgres:postgres@localhost:55432/mailstore_dev_shard1"
      ),
    create: true
  }
]

config :logger, level: :info

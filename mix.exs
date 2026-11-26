defmodule Mailstore.MixProject do
  use Mix.Project

  def project do
    [
      app: :mailstore,
      version: "0.1.0",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Mailstore.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ecto_sql, "~> 3.14"},
      {:postgrex, "~> 0.22"},
      {:jason, "~> 1.4"},
      {:ranch, "~> 2.4"},
      {:telemetry, "~> 1.4"},
      # Ingest from Kafka. Pinned to releases at least two weeks old.
      {:broadway, "== 1.3.1"},
      {:broadway_kafka, "== 0.7.1"},
      {:brod, "== 4.7.0"},
      # Cold tier: S3 API (MinIO, AWS S3), SigV4 signing built in.
      {:req, "== 0.7.5"}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "ecto.setup"],
      # The directory, then the bootstrap shard and every shard (priv/shard/migrations).
      "ecto.setup": ["ecto.create", "mailstore.migrate"],
      "ecto.reset": ["mailstore.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "mailstore.migrate --quiet", "test"]
    ]
  end
end

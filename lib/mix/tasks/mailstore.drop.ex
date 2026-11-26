defmodule Mix.Tasks.Mailstore.Drop do
  @shortdoc "Drop the shard databases and the directory (development and tests only)"
  @moduledoc """
      mix mailstore.drop

  Drops every shard database the directory lists, plus the bootstrap shards of the
  configuration, then the directory itself. `mix ecto.drop` alone would leave the shard
  databases behind with data no directory points at. Refuses in `:prod`.
  """
  use Mix.Task

  @impl true
  def run(_args) do
    if Mix.env() == :prod, do: Mix.raise("mailstore.drop refuses to run in prod")
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:ecto_sql)

    registered =
      case Mailstore.Repo.start_link() do
        {:ok, pid} ->
          urls =
            try do
              List.flatten(Mailstore.Repo.query!("SELECT url FROM shards").rows)
            rescue
              _ -> []
            end

          # Our own connection would keep the directory from being dropped.
          Supervisor.stop(pid)
          urls

        _ ->
          []
      end

    bootstrap = for s <- Application.get_env(:mailstore, :bootstrap_shards, []), do: s.url

    for url <- Enum.uniq(registered ++ bootstrap), is_binary(url) do
      opts = Ecto.Repo.Supervisor.parse_url(url)

      case Ecto.Adapters.Postgres.storage_down(opts) do
        :ok -> Mix.shell().info("dropped #{opts[:database]}")
        {:error, :already_down} -> :ok
        {:error, reason} -> Mix.shell().error("#{opts[:database]}: #{inspect(reason)}")
      end
    end

    Mix.Task.run("ecto.drop")
  end
end

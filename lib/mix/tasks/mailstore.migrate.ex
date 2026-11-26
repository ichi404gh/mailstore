defmodule Mix.Tasks.Mailstore.Migrate do
  @shortdoc "Migrate the directory, register the bootstrap shard, migrate every shard"
  @moduledoc """
      mix mailstore.migrate [--quiet]

  1. The directory database (`Mailstore.Repo`, `priv/repo/migrations`).
  2. The shards of `config :mailstore, :bootstrap_shards` the directory does not list
     yet are registered (their databases created when the entry says `create: true`).
  3. Every registered shard (`priv/shard/migrations`).

  Deploys run this instead of `mix ecto.migrate`, which only knows the directory; a
  shard left behind would fail on the new code. Starts only the repos, not the server.
  """
  use Mix.Task

  alias Mailstore.{Repo, Shards}

  @impl true
  def run(args) do
    quiet = "--quiet" in args
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    started = [start(Repo, [])]

    path = Application.app_dir(:mailstore, "priv/repo/migrations")
    report(quiet, "directory", Ecto.Migrator.run(Repo, path, :up, all: true, log: false))

    # The pool supervisor before the registry, which starts the shard repos under it.
    sup = start(DynamicSupervisor, name: Shards.RepoSupervisor, strategy: :one_for_one)
    started = [start(Shards.Registry, nil), sup | started]

    for id <- Shards.bootstrap(), do: report(quiet, "shard #{id}", :registered)
    for {repo, applied} <- Shards.migrate_all(), do: report(quiet, inspect(repo), applied)

    # Leave nothing running: `mix test` starts the application right after this.
    for pid <- started, is_pid(pid), do: stop(pid)
  end

  defp start(mod, arg) do
    case mod.start_link(arg) do
      {:ok, pid} -> pid
      {:error, {:already_started, _}} -> nil
    end
  end

  defp stop(pid) do
    Process.unlink(pid)
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  defp report(true, _what, _result), do: :ok
  defp report(_, what, :registered), do: Mix.shell().info("#{what}: registered")
  defp report(_, what, []), do: Mix.shell().info("#{what}: up to date")
  defp report(_, what, versions), do: Mix.shell().info("#{what}: applied #{inspect(versions)}")
end

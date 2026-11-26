defmodule Mailstore.DataCase do
  @moduledoc """
  No SQL sandbox on purpose: actors are separate processes and some tests run real
  concurrent transactions. Each test stops all actors and truncates the tables of the
  directory (mailstore_test) and of every shard (mailstore_test_shard1, ...).
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      alias Mailstore.Repo
      import Mailstore.Fixtures
    end
  end

  setup do
    sup = Mailstore.Mailbox.AccountSupervisor

    for {_, pid, _, _} <- DynamicSupervisor.which_children(sup), is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end

    # The directory (ids restart) and every shard's mail. Shards keep their folder id
    # ranges (no RESTART IDENTITY) and the registry stays.
    Mailstore.Repo.query!(
      "TRUNCATE tenants, domains, accounts, forwardings, delivery_rules RESTART IDENTITY CASCADE"
    )

    for repo <- Mailstore.Shards.all(),
        do:
          repo.query!(
            "TRUNCATE folders, messages, message_bodies, cold_bodies, cold_packs, vanished, deliveries"
          )

    Mailstore.Directory.Cache.reset()
    Mailstore.Mailbox.ensure_partitions()
    :ok
  end
end

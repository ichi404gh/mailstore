defmodule Mailstore.DataCase do
  @moduledoc """
  No SQL sandbox on purpose: actors are separate processes and some tests run real
  concurrent transactions. Each test stops all actors and truncates the tables.
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

    Mailstore.Repo.query!(
      "TRUNCATE tenants, domains, accounts, forwardings, folders, messages, message_bodies, vanished, deliveries, delivery_rules RESTART IDENTITY CASCADE"
    )

    Mailstore.Directory.Cache.reset()
    Mailstore.Mailbox.ensure_partitions()
    :ok
  end
end

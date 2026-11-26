defmodule Mailstore.Delivery do
  @moduledoc """
  What LMTP + a Dovecot sieve script do in a classic setup, as a function: store a message for a
  recipient and apply routing rules.

  Input: the inbound queue (`Mailstore.Ingest`), fed by the SMTP layer, which has already
  classified the message (warmup, reply, ...).

  * Master inbox: a copy is appended to the target account; warmup is filtered by
    `only_classes`.
  * `redirect` mode: the original is not kept.
  * External forward: returned as `{:external, address}`. Sending (SRS, ARC, outbound
    queue) is out of scope for this prototype.
  * Loop protection: copies are appended directly and never re-routed (one hop).

  The original and its copies are stored in one transaction, together with everything
  else in the batch (`Mailstore.Mailbox.deliver/1`).
  """
  alias Mailstore.{Directory, Mailbox}
  alias Mailstore.Core.NewMessage

  @type result ::
          {:stored, account_id :: integer, folder :: String.t(), uid :: integer}
          | {:duplicate, account_id :: integer}
          | {:external, String.t()}
          | {:failed, integer, term}

  @doc """
  Deliver one message. Options: `:class`, `:internal_date`, `:key` (identifies the
  delivery: the same key to the same account is stored once).
  """
  @spec deliver(binary, String.t(), keyword) :: {:ok, [result]} | {:error, :unknown_recipient}
  def deliver(raw, rcpt, opts \\ []) do
    [result] = deliver_many([Map.new(opts) |> Map.merge(%{raw: raw, rcpt: rcpt})])
    result
  end

  @doc """
  Deliver many messages in one group commit per shard. Each entry: `%{rcpt, raw}` or
  `%{rcpt, msg}` (a ready `NewMessage`), plus optional `class`, `internal_date`, `key`.
  Returns one result per entry, in order: `{:ok, [result]}` or `{:error, :unknown_recipient}`.
  """
  def deliver_many(entries) do
    plans = Enum.map(entries, &plan/1)

    items =
      for {:ok, targets, _external} <- plans, target <- targets, do: target

    {:ok, results} = if items == [], do: {:ok, []}, else: Mailbox.deliver(items)

    {outcomes, []} =
      Enum.map_reduce(plans, results, fn
        {:ok, targets, external}, results ->
          {mine, rest} = Enum.split(results, length(targets))
          {{:ok, mine ++ Enum.map(external, &{:external, &1})}, rest}

        {:error, _} = error, results ->
          {error, results}
      end)

    outcomes
  end

  # {:ok, store items, external addresses} for one message.
  defp plan(entry) do
    class = Map.get(entry, :class, :other)

    with {:ok, account} <- Directory.lookup_account(entry.rcpt) do
      msg =
        entry[:msg] ||
          NewMessage.from_raw(entry.raw, class: class, internal_date: entry[:internal_date])

      rules = Directory.forwardings_for(account.id, class)
      keep? = not Enum.any?(rules, &(&1.mode == "redirect"))

      item = fn account_id, tenant_id ->
        %{account_id: account_id, tenant_id: tenant_id, msg: msg, key: entry[:key]}
      end

      copies =
        for %{target_account_id: id} <- rules,
            is_integer(id),
            target = Directory.get_account(id),
            target != nil,
            do: item.(id, target.tenant_id)

      external = for %{target_address: address} <- rules, is_binary(address), do: address
      original = if keep?, do: [item.(account.id, account.tenant_id)], else: []
      {:ok, original ++ copies, external}
    end
  end
end

defmodule Mailstore.Delivery do
  @moduledoc """
  What LMTP + a Dovecot Sieve script do in a classic setup, as functions: decide what
  happens to a message for a recipient (`plan/1`), then store it (`store/1`).

  Input: the inbound queue (`Mailstore.Ingest`), fed by the SMTP layer, which has
  already classified the message (warmup, reply, ...).

  * Delivery rules (`Mailstore.Core.Rules`, data in the directory) run first: tag
    warmup vendors, discard, redirect bounces, keep exclusions out of forwarding.
  * Forwarding (`Directory.forwardings_for/2`): a copy for a master inbox (`copy`) or
    only the copy (`redirect`); warmup is filtered by `only_classes`. A copy is stored
    directly in the target mailbox and never routed again (one hop, no loops).
  * External targets come back as `{:external, address}`. Sending (SRS, ARC, outbound
    queue) is out of scope for this prototype.

  The original and its copies are stored in one transaction, together with everything
  else in the batch (`Mailstore.Mailbox.deliver/1`).
  """
  alias Mailstore.{Directory, Mailbox}
  alias Mailstore.Core.{NewMessage, Rules}

  @type result ::
          {:stored, account_id :: integer, folder :: String.t(), uid :: integer}
          | {:duplicate, account_id :: integer}
          | {:discarded, account_id :: integer}
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
  Deliver many messages in one group commit per shard. Each entry: `%{rcpt, raw}`, plus
  optional `class`, `internal_date`, `key`. One result per entry, in order:
  `{:ok, [result]}` or `{:error, :unknown_recipient}`.
  """
  def deliver_many(entries), do: entries |> Enum.map(&plan/1) |> store()

  @doc """
  Everything up to the write, and the CPU-heavy part (rules, MIME, envelope, zstd), so
  ingest runs it in parallel processors. Returns `{:ok, plan}` or
  `{:error, :unknown_recipient}`; the plan carries the items to store, external
  targets, whether the original was discarded, and the names of the rules that matched.
  """
  def plan(entry) do
    with {:ok, account} <- Directory.lookup_account(entry.rcpt) do
      class = Map.get(entry, :class, :other)
      base = NewMessage.from_raw(entry.raw, class: class, internal_date: entry[:internal_date])

      outcome =
        Rules.evaluate(Directory.rules_for(account), entry.raw,
          class: class,
          to_address: entry.rcpt,
          forwardings: forwardings(account, class),
          tree: base.bodystructure
        )

      emit_matched(outcome.matched)

      kept = if outcome.keep, do: [{account.id, account.tenant_id, outcome.keep}], else: []

      {copies, external} =
        Enum.reduce(outcome.copies, {[], []}, fn
          {{:account, id, tenant_id}, bytes}, {copies, ext} ->
            {[{id, tenant_id, bytes} | copies], ext}

          {{:address, address}, bytes}, {copies, ext} ->
            # A redirect to one of our own mailboxes (e.g. the bounce mailbox) is stored.
            case Directory.lookup_account(address) do
              {:ok, target} -> {[{target.id, target.tenant_id, bytes} | copies], ext}
              _ -> {copies, [address | ext]}
            end
        end)

      targets = kept ++ Enum.reverse(copies)
      msgs = messages(targets, base, entry, outcome.class)

      {:ok,
       %{
         account_id: account.id,
         items:
           for({id, tenant_id, bytes} <- targets, do: item(id, tenant_id, msgs[bytes], entry)),
         external: Enum.reverse(external),
         discarded: outcome.discarded,
         matched: outcome.matched
       }}
    end
  end

  @doc "Store planned deliveries: one group commit per shard for all of them."
  def store(plans) do
    items = for {:ok, plan} <- plans, item <- plan.items, do: item
    {:ok, results} = if items == [], do: {:ok, []}, else: Mailbox.deliver(items)

    {outcomes, []} =
      Enum.map_reduce(plans, results, fn
        {:ok, plan}, results ->
          {mine, rest} = Enum.split(results, length(plan.items))
          discarded = if plan.discarded, do: [{:discarded, plan.account_id}], else: []
          {{:ok, mine ++ discarded ++ Enum.map(plan.external, &{:external, &1})}, rest}

        {:error, _} = error, results ->
          {error, results}
      end)

    outcomes
  end

  # Forwarding targets that fire for this class, as Rules expects them.
  defp forwardings(account, class) do
    for rule <- Directory.forwardings_for(account.id, class),
        target = target(rule),
        target != nil,
        do: %{target: target, mode: rule.mode}
  end

  defp target(%{target_account_id: id}) when is_integer(id) do
    case Directory.get_account(id) do
      nil -> nil
      account -> {:account, id, account.tenant_id}
    end
  end

  defp target(%{target_address: address}) when is_binary(address), do: {:address, address}
  defp target(_), do: nil

  # One NewMessage per distinct byte string. Rules mostly add header lines on top
  # (tags, the forward header): such a variant is derived from the message parsed
  # once, with shifted offsets; anything else is parsed again.
  defp messages(targets, base, entry, class) do
    targets
    |> Enum.map(&elem(&1, 2))
    |> Enum.uniq()
    |> Map.new(fn bytes ->
      prefix = byte_size(bytes) - byte_size(base.raw)

      msg =
        if prefix >= 0 and binary_part(bytes, prefix, byte_size(base.raw)) == base.raw do
          NewMessage.prepend(base, binary_part(bytes, 0, prefix), class)
        else
          NewMessage.from_raw(bytes, class: class, internal_date: entry[:internal_date])
        end

      {bytes, NewMessage.compress(msg)}
    end)
  end

  defp item(account_id, tenant_id, msg, entry),
    do: %{account_id: account_id, tenant_id: tenant_id, msg: msg, key: entry[:key]}

  defp emit_matched([]), do: :ok

  defp emit_matched(names) do
    for name <- names,
        do: :telemetry.execute([:mailstore, :delivery, :rule], %{count: 1}, %{rule: name})

    :ok
  end
end

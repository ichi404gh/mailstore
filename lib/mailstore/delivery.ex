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
  * External targets come back as `{:external, address}` and are sent out
    (`forward/1`, `Mailstore.Outbound`) before anything is stored.

  The original and its copies are stored in one transaction, together with everything
  else in the batch and the keys of its sent forwards (`Mailstore.Mailbox.deliver/1`).
  """
  alias Mailstore.{Directory, Mailbox, Outbound}
  alias Mailstore.Core.{NewMessage, Rules}

  @type result ::
          {:stored, account_id :: integer, folder :: String.t(), uid :: integer}
          | {:duplicate, account_id :: integer}
          | {:discarded, account_id :: integer}
          | {:external, String.t()}
          | {:failed, integer, term}

  @doc """
  Deliver one message. Options: `:class`, `:internal_date`, `:key` (identifies the
  delivery: the same key to the same account is stored once, and forwarded once),
  `:sender` (the envelope sender, used for forwards).
  """
  @spec deliver(binary, String.t(), keyword) :: {:ok, [result]} | {:error, :unknown_recipient}
  def deliver(raw, rcpt, opts \\ []) do
    [result] = deliver_many([Map.new(opts) |> Map.merge(%{raw: raw, rcpt: rcpt})])
    result
  end

  @doc """
  Deliver many messages in one group commit per shard. Each entry: `%{rcpt, raw}`, plus
  optional `class`, `internal_date`, `key`, `sender`. One result per entry, in order:
  `{:ok, [result]}` or `{:error, :unknown_recipient}`.
  """
  def deliver_many(entries), do: entries |> Enum.map(&plan/1) |> forward() |> store()

  @doc """
  Everything up to the write, and the CPU-heavy part (rules, MIME, envelope, zstd), so
  ingest runs it in parallel processors. Returns `{:ok, plan}` or
  `{:error, :unknown_recipient}`; the plan carries the items to store, the forwards to
  external targets (`Outbound.new/1`), whether the original was discarded, and the
  names of the rules that matched.
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
              _ -> {copies, [{address, bytes} | ext]}
            end
        end)

      targets = kept ++ Enum.reverse(copies)
      msgs = messages(targets, base, entry, outcome.class)

      {:ok,
       %{
         account_id: account.id,
         items:
           for({id, tenant_id, bytes} <- targets, do: item(id, tenant_id, msgs[bytes], entry)),
         external:
           for(
             {address, bytes} <- Enum.reverse(external),
             do: forward(account, address, bytes, entry, outcome.class)
           ),
         discarded: outcome.discarded,
         matched: outcome.matched
       }}
    end
  end

  @doc """
  Send the plans' forwards out (`Outbound.deliver/1`), except those a previous run
  already sent (their key is in `deliveries`). Returns the plans with the sent forwards
  marked `sent`, for `store/1` to record. Raises
  `Outbound.Error` or a database error: nothing is stored then, and running it again
  is the retry. With outbound off, forwards are counted as dropped and left unmarked.
  """
  def forward(plans) do
    forwards = for {:ok, plan} <- plans, f <- plan.external, do: f

    cond do
      forwards == [] ->
        plans

      not Outbound.enabled?() ->
        Outbound.dropped(forwards)
        plans

      true ->
        sent =
          Mailbox.delivered_keys(for f <- forwards, f.key, do: {f.tenant_id, f.account_id, f.key})

        forwards
        |> Enum.reject(&(&1.key && MapSet.member?(sent, {&1.account_id, &1.key})))
        # The same record twice in one batch: one forward.
        |> Enum.uniq_by(&((&1.key && {&1.account_id, &1.key}) || make_ref()))
        |> Outbound.deliver()

        Enum.map(plans, fn
          {:ok, plan} ->
            {:ok, %{plan | external: Enum.map(plan.external, &Map.put(&1, :sent, true))}}

          error ->
            error
        end)
    end
  end

  @doc """
  Store planned deliveries: one group commit per shard for all of them, with the keys
  of the forwards `forward/1` sent.
  """
  def store(plans) do
    plans =
      Enum.map(plans, fn
        {:ok, plan} -> {:ok, Map.put(plan, :marks, marks(plan))}
        error -> error
      end)

    items = for {:ok, plan} <- plans, item <- plan.items ++ plan.marks, do: item
    {:ok, results} = if items == [], do: {:ok, []}, else: Mailbox.deliver(items)

    {outcomes, []} =
      Enum.map_reduce(plans, results, fn
        {:ok, plan}, results ->
          {mine, rest} = Enum.split(results, length(plan.items) + length(plan.marks))
          mine = Enum.take(mine, length(plan.items))
          discarded = if plan.discarded, do: [{:discarded, plan.account_id}], else: []
          {{:ok, mine ++ discarded ++ Enum.map(plan.external, &{:external, &1.target})}, rest}

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

  defp forward(account, address, bytes, entry, class) do
    Outbound.new(%{
      account_id: account.id,
      tenant_id: account.tenant_id,
      delivery_key: entry[:key],
      rcpt: entry.rcpt,
      target: address,
      sender: entry[:sender],
      raw: bytes,
      class: class,
      received_at: entry[:internal_date]
    })
  end

  # Sent forwards, recorded like deliveries to the source account.
  defp marks(plan) do
    for %{sent: true, key: key} = f <- plan.external,
        key != nil,
        do: %{account_id: f.account_id, tenant_id: f.tenant_id, key: key, mark: true}
  end

  defp emit_matched([]), do: :ok

  defp emit_matched(names) do
    for name <- names,
        do: :telemetry.execute([:mailstore, :delivery, :rule], %{count: 1}, %{rule: name})

    :ok
  end
end

defmodule Mailstore.Delivery do
  @moduledoc """
  What LMTP + a Dovecot sieve script do in a classic setup, as a function: store a message for a
  recipient and apply routing rules.

  Intended input: a message queue fed by the inbound SMTP layer, which has already
  classified the message (warmup, reply, ...).

  * Master inbox: a copy is appended to the target account; warmup is filtered by
    `only_classes`.
  * `redirect` mode: the original is not kept.
  * External forward: returned as `{:external, address}`. Sending (SRS, ARC, outbound
    queue) is out of scope for this prototype.
  * Loop protection: copies are appended directly and never re-routed (one hop).
  """
  alias Mailstore.{Directory, Mailbox}
  alias Mailstore.Core.NewMessage

  @type result ::
          {:stored, account_id :: integer, folder :: String.t(), uid :: integer}
          | {:external, String.t()}
          | {:failed, integer, term}

  @spec deliver(binary, String.t(), keyword) :: {:ok, [result]} | {:error, :unknown_recipient}
  def deliver(raw, rcpt, opts \\ []) do
    class = Keyword.get(opts, :class, :other)

    with {:ok, account} <- Directory.lookup_account(rcpt) do
      msg = NewMessage.from_raw(raw, class: class, internal_date: opts[:internal_date])

      rules = Directory.forwardings_for(account.id, class)

      keep? = not Enum.any?(rules, &(&1.mode == "redirect"))

      original = if keep?, do: [store(account.id, "INBOX", msg)], else: []

      routed =
        Enum.map(rules, fn
          %{target_account_id: id} when is_integer(id) -> store(id, "INBOX", msg)
          %{target_address: address} -> {:external, address}
        end)

      {:ok, original ++ routed}
    end
  end

  defp store(account_id, folder, msg) do
    case Mailbox.append(account_id, folder, [msg]) do
      {:ok, [uid]} -> {:stored, account_id, folder, uid}
      {:error, reason} -> {:failed, account_id, reason}
    end
  end
end

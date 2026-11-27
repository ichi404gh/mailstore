defmodule Mailstore.Outbound do
  @moduledoc """
  Outbound mail: copies for forwarding targets outside our mailboxes (CRM integrations,
  personal mailboxes). This context decides what goes out and makes sure it goes out
  once; how it goes out is an adapter's job (`Outbound.Adapter`):

    * `Outbound.Kafka` - a record in the relay sender's Kafka topic; the relay sender
      (a separate service) sends it through an SMTP relay. The only adapter so far;
    * an SMTP adapter of our own could send directly later, behind the same calls.

  How forwards flow:

  * `Delivery.plan/1` turns every external target into a forward (`new/1`): the bytes
    to send (the copy the delivery rules made, with their forward headers), the
    envelope sender of the original, the target and a key.
  * No SRS and no ARC: the copy goes out with the original envelope sender, as
    forwarding through the relay works today (its DKIM signer signs for the `From`
    domain).
  * `deliver/1` returns only once the adapter has taken every forward for good (for
    Kafka: acknowledged by the broker). Ingest hands a batch's forwards over before it
    stores the batch and acks its offsets (`Delivery.forward/1`), so a forward is never
    lost.
  * Idempotency: a forward's key (`key/2`: the delivery key and the target) is recorded
    in `deliveries` of the source account, in the transaction that stores the batch. A
    replayed batch finds it there and does not hand it over again. A crash between the
    hand-over and that commit sends the forward twice: the one window for a duplicate.

  Off unless configured (`config :mailstore, :outbound`, see `config/config.exs`).
  While off, external targets are only reported (`{:external, address}`) and counted
  as dropped (`[:mailstore, :outbound, :dropped]`); nothing is recorded, so turning it
  on later does not skip anything that is replayed.

  Events: `[:mailstore, :outbound, :deliver]` (`%{count, bytes, duration}`, metadata
  `%{adapter}`) and `[:mailstore, :outbound, :dropped]` (`%{count}`).
  """
  use Supervisor

  defmodule Error do
    @moduledoc """
    Forwards could not be handed over. `transient: true` (the transport is unavailable,
    a timeout): retry later. `false`: this forward will never go through as it is (too
    large for the transport).
    """
    defexception [:message, :reason, transient: true]
  end

  @type forward :: %{
          account_id: integer,
          tenant_id: integer,
          key: binary | nil,
          rcpt: String.t(),
          target: String.t(),
          sender: String.t() | nil,
          raw: binary,
          class: atom,
          received_at: DateTime.t() | nil
        }

  def config, do: Application.get_env(:mailstore, :outbound, [])

  def enabled?, do: Keyword.get(config(), :enabled, false)

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    config = Keyword.merge(config(), opts)
    Supervisor.init(adapter(config).children(config), strategy: :one_for_one)
  end

  @doc """
  A forward of `raw` to `target`. Fields: `account_id`, `tenant_id` (the source
  account, where the key is recorded), `rcpt` (the original recipient), `target`,
  `raw`, `class`, and optional `delivery_key`, `sender` (envelope sender of the
  original; nil or "" for a bounce), `received_at`.
  """
  @spec new(map) :: forward
  def new(fields) do
    fields
    |> Map.take([:account_id, :tenant_id, :rcpt, :target, :raw, :class, :sender, :received_at])
    |> Map.put(:key, key(fields[:delivery_key], fields.target))
  end

  @doc """
  Key of a forward: the delivery key and the target, 128 bits. Nil without a delivery
  key (such a forward is sent every time).
  """
  def key(nil, _target), do: nil

  def key(delivery_key, target) do
    <<key::binary-16, _::binary>> =
      :crypto.hash(:sha256, ["forward", 0, delivery_key, 0, String.downcase(target)])

    key
  end

  @doc """
  Hand forwards to the adapter; returns `:ok` once it has taken every one of them,
  raises `Error` otherwise (some may have gone out: handing them over again is how to
  retry).
  """
  @spec deliver([forward]) :: :ok
  def deliver([]), do: :ok

  def deliver(forwards) do
    config = config()
    adapter = adapter(config)
    started = System.monotonic_time()

    case adapter.deliver(config, forwards) do
      :ok ->
        :telemetry.execute(
          [:mailstore, :outbound, :deliver],
          %{
            count: length(forwards),
            bytes: Enum.sum_by(forwards, &byte_size(&1.raw)),
            duration: System.monotonic_time() - started
          },
          %{adapter: adapter}
        )

        :ok

      {:error, %Error{} = e} ->
        raise e

      {:error, reason} ->
        raise Error, message: "forwards not handed over: #{inspect(reason)}", reason: reason
    end
  end

  @doc "Forwards that were not sent because outbound is off."
  def dropped([]), do: :ok

  def dropped(forwards),
    do: :telemetry.execute([:mailstore, :outbound, :dropped], %{count: length(forwards)}, %{})

  defp adapter(config), do: Keyword.fetch!(config, :adapter)
end

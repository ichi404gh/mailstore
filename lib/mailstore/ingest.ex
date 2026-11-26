defmodule Mailstore.Ingest do
  @moduledoc """
  Context: the inbound queue. Kafka topics filled by the SMTP layer are consumed by
  `Ingest.Pipeline` (Broadway) and stored by `Mailstore.Delivery`. Off unless
  `config :mailstore, :ingest, enabled: true`.
  """
  use Supervisor

  def enabled?, do: Keyword.get(config(), :enabled, false)

  def config, do: Application.get_env(:mailstore, :ingest, [])

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    config = Keyword.merge(config(), opts)

    children =
      if(config[:dead_letter_topic],
        do: [{Mailstore.Ingest.DeadLetter, Keyword.fetch!(config, :hosts)}],
        else: []
      ) ++ [{Mailstore.Ingest.Pipeline, config}]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

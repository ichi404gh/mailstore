defmodule Mix.Tasks.Loadgen.Poller do
  @shortdoc "Persistent IMAP sessions polling with a command cycle; measure server CPU"
  @moduledoc """
      mix loadgen.poller --sessions 5000 --users 5000 --interval 5 --duration 60 \\
        --cycle noop,search_new,fetch_new --deliver-rate 50 --server-node mailstore@host

  Options:
  #{Loadgen.CLI.common_help()}
    --interval SECONDS      each session runs its cycle this often (default 10)
    --cycle LIST            steps, see Loadgen.PollerSession (default noop,search_new,fetch_new)
    --duration SECONDS      measurement window after the ramp and warm-up (default 60)
    --deliver-rate N        new messages/s delivered to random users via :erpc (default 0)
    --deliver-lmtp H:P      deliver over LMTP instead (servers without :erpc, e.g. Stalwart)
    --reconnect-mean SECS   reconnect after ~this lifetime (0 = never, default 0)
    --pg-container NAME     docker container of Postgres, to measure its CPU (default none)

  One session per user (INBOX only), unlike `loadgen.idle`.
  """
  use Mix.Task

  @impl true
  def run(args) do
    cfg =
      Loadgen.CLI.parse(
        args,
        [
          interval: :integer,
          cycle: :string,
          duration: :integer,
          deliver_rate: :integer,
          deliver_lmtp: :string,
          reconnect_mean: :integer,
          pg_container: :string
        ],
        interval: 10,
        cycle: "noop,search_new,fetch_new",
        duration: 60,
        deliver_rate: 0,
        deliver_lmtp: "",
        reconnect_mean: 0,
        pg_container: ""
      )

    cfg
    |> Map.put(:interval_ms, cfg.interval * 1000)
    |> Map.put(:reconnect_mean_ms, cfg.reconnect_mean * 1000)
    |> Map.put(:cycle, String.split(cfg.cycle, ","))
    |> Loadgen.Poller.run()
  end
end

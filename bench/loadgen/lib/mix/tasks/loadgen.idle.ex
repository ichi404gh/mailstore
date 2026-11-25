defmodule Mix.Tasks.Loadgen.Idle do
  @shortdoc "Open N IMAP sessions in IDLE and measure server memory"
  @moduledoc """
      mix loadgen.idle --sessions 10000 --users 5000 --rate 1000 --hold 30

  Options:
  #{Loadgen.CLI.common_help()}
    --folders LIST          default "INBOX,Junk" (sessions per user = number of folders)
    --hold SECONDS          keep sessions open after the ramp
    --until STEP            stop setup after connect|login|select|idle (default idle)
  """
  use Mix.Task

  @impl true
  def run(args) do
    args
    |> Loadgen.CLI.parse([hold: :integer, until: :string], hold: 30, until: "idle")
    |> Loadgen.Idle.run()
  end
end

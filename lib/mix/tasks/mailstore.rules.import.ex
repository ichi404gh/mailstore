defmodule Mix.Tasks.Mailstore.Rules.Import do
  @shortdoc "Replace the global delivery rules with the ones in a JSON file"
  @moduledoc """
      mix mailstore.rules.import priv/delivery_rules/example.json

  The file is `{"rules": [...]}` (see `Mailstore.Core.Rules` for the format). All rules
  are validated first; the global set is swapped in one transaction or not at all.
  Tenant and account rules are not touched. Running nodes pick the change up with
  their next directory poll (about a second).
  """
  use Mix.Task

  @impl true
  def run([path]) do
    Mix.Task.run("app.config")

    Application.put_env(
      :mailstore,
      :imap,
      Keyword.put(Application.get_env(:mailstore, :imap, []), :enabled, false)
    )

    {:ok, _} = Application.ensure_all_started(:mailstore)

    %{"rules" => rules} = path |> File.read!() |> Jason.decode!()

    case Mailstore.Directory.import_global_rules(rules) do
      {:ok, n} -> Mix.shell().info("imported #{n} global rules from #{path}")
      {:error, name, reason} -> Mix.raise("rule #{name} is invalid: #{reason}")
    end
  end

  def run(_), do: Mix.raise("usage: mix mailstore.rules.import FILE")
end

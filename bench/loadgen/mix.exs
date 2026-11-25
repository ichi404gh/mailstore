defmodule Loadgen.MixProject do
  use Mix.Project

  # Load generator for the mailstore IMAP server. A separate project on purpose: it runs
  # on load-generator machines without the server code or a database, and never ends up
  # in the server release. No dependencies beyond OTP.
  def project do
    [app: :loadgen, version: "0.1.0", elixir: "~> 1.20", deps: []]
  end

  def application, do: [extra_applications: [:logger, :ssl]]
end

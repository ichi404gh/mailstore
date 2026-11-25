defmodule Mailstore.Core.Flags do
  @moduledoc """
  IMAP system flags packed into a smallint; everything else is a keyword.
  Keywords are kept sorted and unique so the database can compare them cheaply.
  """
  import Bitwise

  @system [
    {"\\Seen", 1},
    {"\\Answered", 2},
    {"\\Flagged", 4},
    {"\\Deleted", 8},
    {"\\Draft", 16}
  ]
  @by_name Map.new(@system, fn {name, bit} -> {String.downcase(name), bit} end)

  def seen, do: 1
  def deleted, do: 8

  @doc "Flag list -> {mask, keywords}. Unknown backslash flags (\\Recent etc.) are ignored."
  def parse(flags) when is_list(flags) do
    Enum.reduce(flags, {0, []}, fn flag, {mask, kws} ->
      case Map.fetch(@by_name, String.downcase(flag)) do
        {:ok, bit} -> {mask ||| bit, kws}
        :error -> if String.starts_with?(flag, "\\"), do: {mask, kws}, else: {mask, [flag | kws]}
      end
    end)
    |> then(fn {mask, kws} -> {mask, kws |> Enum.uniq() |> Enum.sort()} end)
  end

  def to_list(mask, keywords \\ []) do
    for({name, bit} <- @system, (mask &&& bit) != 0, do: name) ++ keywords
  end

  def seen?(mask), do: (mask &&& 1) != 0
end

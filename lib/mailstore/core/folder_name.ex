defmodule Mailstore.Core.FolderName do
  @moduledoc """
  Folder names: hierarchy separator "/", INBOX rules, validation.

  Names are stored as the client sends them: IMAP4rev1 names are modified UTF-7
  (RFC 3501 5.1.3), so they are 7-bit; 8-bit names are rejected, as Dovecot does.
  """
  @sep "/"
  @max_bytes 1024

  def separator, do: @sep

  @doc """
  Canonical form of a name from a client, or `{:error, :invalid_name}`.

  A trailing separator only says the name may get children (`CREATE foo/`), so it is
  dropped. INBOX is case-insensitive, also as the first component (`inbox/x` is
  `INBOX/x`).
  """
  def normalize(name) when is_binary(name) do
    name = String.trim_trailing(name, @sep)

    cond do
      not valid?(name) ->
        {:error, :invalid_name}

      String.upcase(name) == "INBOX" ->
        {:ok, "INBOX"}

      String.upcase(name) |> String.starts_with?("INBOX" <> @sep) ->
        {:ok, "INBOX" <> trim_inbox(name)}

      true ->
        {:ok, name}
    end
  end

  defp trim_inbox(<<_::binary-size(5), rest::binary>>), do: rest

  defp valid?(name) do
    name != "" and byte_size(name) <= @max_bytes and
      not String.starts_with?(name, @sep) and
      not String.contains?(name, [@sep <> @sep, "*", "%"]) and
      ascii_printable?(name)
  end

  defp ascii_printable?(<<>>), do: true

  defp ascii_printable?(<<c, rest::binary>>) when c >= 0x20 and c < 0x7F,
    do: ascii_printable?(rest)

  defp ascii_printable?(_), do: false

  @doc ~s(Ancestors of a name, outermost first: "a/b/c" -> ["a", "a/b"].)
  def parents(name) do
    parts = String.split(name, @sep)

    for n <- 1..(length(parts) - 1)//1,
        do: parts |> Enum.take(n) |> Enum.join(@sep)
  end

  @doc "Is `name` the folder `parent` or inside it?"
  def within?(name, parent), do: name == parent or String.starts_with?(name, parent <> @sep)

  @doc "Name after moving `parent` (and so `name` inside it) to `new_parent`."
  def reparent(name, parent, new_parent) do
    new_parent <> binary_part(name, byte_size(parent), byte_size(name) - byte_size(parent))
  end
end

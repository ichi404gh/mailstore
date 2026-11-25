defmodule Mailstore.Imap.Format do
  @moduledoc "Rendering IMAP data items: strings, flag lists, dates, ENVELOPE."
  alias Mailstore.Core.Address

  @months ~w(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)

  @doc "nstring: NIL, a quoted string when safe, otherwise a literal."
  def string(nil), do: "NIL"

  def string(s) when is_binary(s) do
    if quotable?(s), do: [?", escape(s), ?"], else: literal(s)
  end

  def literal(s), do: ["{", Integer.to_string(byte_size(s)), "}\r\n", s]

  defp quotable?(s), do: byte_size(s) <= 1024 and s =~ ~r/^[\x20-\x7e]*$/

  defp escape(s), do: s |> String.replace("\\", "\\\\") |> String.replace(~s("), ~s(\\"))

  def flags(list), do: ["(", Enum.join(list, " "), ")"]

  @doc ~s(date-time: "20-Nov-2026 12:00:00 +0000")
  def datetime(%DateTime{} = dt) do
    dt = DateTime.shift_zone!(dt, "Etc/UTC")

    [
      ?",
      pad(dt.day),
      ?-,
      Enum.at(@months, dt.month - 1),
      ?-,
      Integer.to_string(dt.year),
      ?\s,
      pad(dt.hour),
      ?:,
      pad(dt.minute),
      ?:,
      pad(dt.second),
      " +0000",
      ?"
    ]
  end

  @doc "date-time from APPEND, e.g. \" 5-Nov-2026 12:00:00 +0300\"."
  def parse_datetime(s) do
    re = ~r/^\s?(\d{1,2})-([A-Za-z]{3})-(\d{4}) (\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$/

    with [_, d, mon, y, h, mi, sec, sign, oh, om] <- Regex.run(re, s),
         month when month != nil <- month_number(mon),
         {:ok, naive} <-
           NaiveDateTime.new(int(y), month, int(d), int(h), int(mi), int(sec), {0, 6}) do
      offset = (int(oh) * 60 + int(om)) * 60 * if(sign == "-", do: -1, else: 1)
      {:ok, naive |> NaiveDateTime.add(-offset) |> DateTime.from_naive!("Etc/UTC")}
    else
      _ -> :error
    end
  end

  @doc "date from SEARCH, e.g. 1-Feb-1994."
  def parse_date(s) do
    with [_, d, mon, y] <- Regex.run(~r/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/, s),
         month when month != nil <- month_number(mon),
         {:ok, date} <- Date.new(int(y), month, int(d)) do
      {:ok, date}
    else
      _ -> :error
    end
  end

  @doc "ENVELOPE from the header values stored at delivery."
  def envelope(env) when is_map(env) do
    from = env["from"]

    [
      "(",
      Enum.intersperse(
        [
          string(env["date"]),
          string(env["subject"]),
          addresses(from),
          addresses(env["sender"] || from),
          addresses(env["reply_to"] || from),
          addresses(env["to"]),
          addresses(env["cc"]),
          addresses(env["bcc"]),
          string(env["in_reply_to"]),
          string(env["message_id"])
        ],
        " "
      ),
      ")"
    ]
  end

  def envelope(nil), do: envelope(%{})

  defp addresses(value) do
    case Address.parse_list(value) do
      [] ->
        "NIL"

      list ->
        [
          "(",
          for(
            a <- list,
            do: ["(", string(a.name), " NIL ", string(a.mailbox), " ", string(a.host), ")"]
          ),
          ")"
        ]
    end
  end

  defp month_number(mon) do
    case Enum.find_index(@months, &(String.downcase(&1) == String.downcase(mon))) do
      nil -> nil
      i -> i + 1
    end
  end

  defp pad(n), do: n |> Integer.to_string() |> String.pad_leading(2, "0")
  defp int(s), do: String.to_integer(s)
end

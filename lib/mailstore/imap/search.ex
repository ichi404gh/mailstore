defmodule Mailstore.Imap.Search do
  @moduledoc """
  SEARCH: parse keys into a tree and evaluate it over the selected folder.

  Evaluated over the cheapest data that answers the query (see `needs/1`): UID and
  sequence sets from the session's UID map without the database, pure UNSEEN from the
  partial index, flags/size/dates from narrow rows, headers from the envelope, and
  bodies only for TEXT, BODY and HEADER on non-envelope fields.
  SENTBEFORE/SENTON/SENTSINCE use the internal date as an approximation.
  """
  alias Mailstore.Core.Headers
  alias Mailstore.Core.UidMap
  alias Mailstore.Imap.{Format, SequenceSet}

  @flag_keys %{
    "ANSWERED" => {"\\Answered", true},
    "UNANSWERED" => {"\\Answered", false},
    "DELETED" => {"\\Deleted", true},
    "UNDELETED" => {"\\Deleted", false},
    "DRAFT" => {"\\Draft", true},
    "UNDRAFT" => {"\\Draft", false},
    "FLAGGED" => {"\\Flagged", true},
    "UNFLAGGED" => {"\\Flagged", false},
    "SEEN" => {"\\Seen", true},
    "UNSEEN" => {"\\Seen", false}
  }
  @header_keys %{
    "FROM" => "from",
    "TO" => "to",
    "CC" => "cc",
    "BCC" => "bcc",
    "SUBJECT" => "subject"
  }
  @date_keys %{
    "BEFORE" => :before,
    "ON" => :on,
    "SINCE" => :since,
    "SENTBEFORE" => :before,
    "SENTON" => :on,
    "SENTSINCE" => :since
  }
  # Header fields kept in the envelope column (key in the stored map).
  @envelope_fields %{
    "date" => "date",
    "subject" => "subject",
    "from" => "from",
    "sender" => "sender",
    "reply-to" => "reply_to",
    "to" => "to",
    "cc" => "cc",
    "bcc" => "bcc",
    "in-reply-to" => "in_reply_to",
    "message-id" => "message_id"
  }

  def parse([{:atom, a}, charset | rest]) do
    if String.upcase(a) == "CHARSET" do
      if String.upcase(value(charset)) in ["UTF-8", "US-ASCII"],
        do: parse_keys(rest),
        else: {:error, :badcharset}
    else
      parse_keys([{:atom, a}, charset | rest])
    end
  end

  def parse(tokens), do: parse_keys(tokens)

  defp parse_keys([]), do: {:error, "Missing search keys"}

  defp parse_keys(tokens) do
    with {:ok, keys} <- keys(tokens, []), do: {:ok, {:and, keys}}
  end

  defp keys([], acc), do: {:ok, Enum.reverse(acc)}

  defp keys(tokens, acc) do
    with {:ok, key, rest} <- key(tokens), do: keys(rest, [key | acc])
  end

  defp key([{:list, list} | rest]) do
    with {:ok, keys} <- keys(list, []), do: {:ok, {:and, keys}, rest}
  end

  defp key([{:atom, a} | rest]) do
    up = String.upcase(a)

    cond do
      up == "ALL" or up == "OLD" ->
        {:ok, :all, rest}

      up == "NEW" or up == "RECENT" ->
        {:ok, :none, rest}

      Map.has_key?(@flag_keys, up) ->
        flag_key(@flag_keys[up], rest)

      up in ["KEYWORD", "UNKEYWORD"] ->
        arg(rest, &{:flag, &1, up == "KEYWORD"})

      Map.has_key?(@header_keys, up) ->
        arg(rest, &{:header, @header_keys[up], &1})

      up == "HEADER" ->
        header(rest)

      up == "BODY" ->
        arg(rest, &{:body, &1})

      up == "TEXT" ->
        arg(rest, &{:text, &1})

      up in ["LARGER", "SMALLER"] ->
        number(rest, up)

      Map.has_key?(@date_keys, up) ->
        date(rest, @date_keys[up])

      up == "UID" ->
        set(rest, :uidset)

      up == "NOT" ->
        with {:ok, k, rest} <- key(rest), do: {:ok, {:not, k}, rest}

      up == "OR" ->
        with {:ok, a, rest} <- key(rest),
             {:ok, b, rest} <- key(rest),
             do: {:ok, {:or, a, b}, rest}

      true ->
        set([{:atom, a} | rest], :seqset)
    end
  end

  defp key(_), do: {:error, "Invalid search key"}

  defp flag_key({name, want}, rest), do: {:ok, {:flag, name, want}, rest}

  defp arg([v | rest], build) when elem(v, 0) in [:atom, :string],
    do: {:ok, build.(value(v)), rest}

  defp arg(_, _), do: {:error, "Missing search argument"}

  defp header([f, v | rest]), do: {:ok, {:header, String.downcase(value(f)), value(v)}, rest}
  defp header(_), do: {:error, "HEADER needs a field and a value"}

  defp number([v | rest], kind) do
    case Integer.parse(value(v)) do
      {n, ""} -> {:ok, {if(kind == "LARGER", do: :larger, else: :smaller), n}, rest}
      _ -> {:error, "Invalid number"}
    end
  end

  defp number(_, _), do: {:error, "Missing number"}

  defp date([v | rest], op) do
    case Format.parse_date(value(v)) do
      {:ok, d} -> {:ok, {:date, op, d}, rest}
      :error -> {:error, "Invalid date"}
    end
  end

  defp date(_, _), do: {:error, "Missing date"}

  defp set([{:atom, s} | rest], kind) do
    case SequenceSet.parse(s) do
      {:ok, ranges} -> {:ok, {kind, ranges}, rest}
      :error -> {:error, "Unknown search key #{s}"}
    end
  end

  defp set(_, _), do: {:error, "Invalid sequence set"}

  defp value({:atom, a}), do: a
  defp value({:string, s}), do: s

  @doc """
  The cheapest data that answers the query:
    * `:ids` - only UID / sequence sets (and ALL, NOT/OR of those): the session's UID
      map is enough, no database
    * `:flags` - also flags, size, dates: narrow rows, no envelope or body
    * `:full` - headers or text: envelope, and bodies if `needs_body?/1`
  """
  def needs(:all), do: :ids
  def needs(:none), do: :ids
  def needs({:seqset, _}), do: :ids
  def needs({:uidset, _}), do: :ids
  def needs({:flag, _, _}), do: :flags
  def needs({:larger, _}), do: :flags
  def needs({:smaller, _}), do: :flags
  def needs({:date, _, _}), do: :flags
  def needs({:not, a}), do: needs(a)
  def needs({:or, a, b}), do: max_need(needs(a), needs(b))
  def needs({:and, keys}), do: Enum.reduce(keys, :ids, &max_need(needs(&1), &2))
  def needs(_header_or_text), do: :full

  defp max_need(a, b) do
    rank = %{ids: 0, flags: 1, full: 2}
    if rank[a] >= rank[b], do: a, else: b
  end

  @doc "Is this exactly `UNSEEN`? (Served from a partial index.)"
  def only_unseen?({:and, [{:flag, "\\Seen", false}]}), do: true
  def only_unseen?(_), do: false

  @doc """
  UIDs that can possibly match, from a top-level UID or sequence set ANDed with the rest
  (e.g. `UID 4380:* UNSEEN`), or `:all`. Narrows what is loaded from the database.
  """
  def candidates({:and, keys}, map) do
    Enum.find_value(keys, :all, fn
      {:uidset, r} -> SequenceSet.uids(r, map)
      {:seqset, r} -> r |> SequenceSet.seqs(UidMap.size(map)) |> Enum.map(&UidMap.uid(map, &1))
      _ -> nil
    end)
  end

  def candidates(_tree, _map), do: :all

  @doc "Does evaluation need message bodies?"
  def needs_body?({:and, keys}), do: Enum.any?(keys, &needs_body?/1)
  def needs_body?({:or, a, b}), do: needs_body?(a) or needs_body?(b)
  def needs_body?({:not, a}), do: needs_body?(a)
  def needs_body?({:body, _}), do: true
  def needs_body?({:text, _}), do: true
  def needs_body?({:header, f, _}), do: not Map.has_key?(@envelope_fields, f)
  def needs_body?(_), do: false

  @doc """
  `msgs`: maps with :seq, :uid, :flags, :size, :internal_date, :envelope and :raw (or nil).
  Returns matching messages in sequence order.
  """
  def run(tree, msgs, %{max_seq: _, max_uid: _} = ctx),
    do: Enum.filter(msgs, &match(tree, &1, ctx))

  defp match(:all, _m, _), do: true
  defp match(:none, _m, _), do: false
  defp match({:and, keys}, m, ctx), do: Enum.all?(keys, &match(&1, m, ctx))
  defp match({:or, a, b}, m, ctx), do: match(a, m, ctx) or match(b, m, ctx)
  defp match({:not, a}, m, ctx), do: not match(a, m, ctx)
  defp match({:flag, name, want}, m, _), do: has_flag?(m.flags, name) == want
  defp match({:larger, n}, m, _), do: m.size > n
  defp match({:smaller, n}, m, _), do: m.size < n
  defp match({:seqset, r}, m, ctx), do: SequenceSet.member?(r, m.seq, ctx.max_seq)
  defp match({:uidset, r}, m, ctx), do: SequenceSet.member?(r, m.uid, ctx.max_uid)
  defp match({:body, s}, m, _), do: contains?(body_text(m.raw), s)
  defp match({:text, s}, m, _), do: contains?(m.raw, s)

  defp match({:date, op, d}, m, _) do
    cmp = Date.compare(DateTime.to_date(m.internal_date), d)

    case op do
      :before -> cmp == :lt
      :on -> cmp == :eq
      :since -> cmp != :lt
    end
  end

  defp match({:header, field, s}, m, _) do
    case header_value(m, field) do
      nil -> false
      value -> s == "" or contains?(value, s)
    end
  end

  defp header_value(m, field) do
    case Map.fetch(@envelope_fields, field) do
      {:ok, key} ->
        (m.envelope || %{})[key]

      :error ->
        values = for {^field, v} <- Headers.parse(m.raw || ""), do: v
        if values == [], do: nil, else: Enum.join(values, " ")
    end
  end

  defp has_flag?(flags, "\\" <> _ = name),
    do: Enum.any?(flags, &(String.downcase(&1) == String.downcase(name)))

  defp has_flag?(flags, name), do: name in flags

  defp body_text(nil), do: ""
  defp body_text(raw), do: raw |> Headers.split() |> elem(1)

  defp contains?(nil, _), do: false
  defp contains?(hay, needle), do: String.contains?(String.downcase(hay), String.downcase(needle))
end

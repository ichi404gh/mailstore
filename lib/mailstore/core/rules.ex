defmodule Mailstore.Core.Rules do
  @moduledoc """
  Delivery rules: what a Sieve script does at delivery, as data instead of a template.

  A rule is a JSON-compatible map, stored in the directory (`delivery_rules`) and
  compiled once when loaded:

      %{"name" => "VendorA", "stage" => "detect", "position" => 20,
        "test" => %{"header" => "Message-ID", "contains" => "<vendor-a-", "case" => "sensitive"},
        "actions" => [%{"add_header" => ["X-Warmup-Vendor", "VendorA"]},
                      %{"set_class" => "warmup"}, "stop"]}

  ## Evaluation

  Two stages, each an ordered list (global, tenant and account rules merged by
  `position`). `detect` runs for every message. `forward` runs only when the mailbox
  has forwarding targets (`Directory.forwardings_for/2`); if no rule in it stops, the
  message is forwarded at its end, like the final `else` of the Sieve script. `stop`
  ends all evaluation.

  As in Sieve: the message is kept in the recipient's mailbox (implicit keep) unless
  `discard`, `redirect` or a redirect-mode forwarding cancels it; `keep` stores it as it
  is at that moment; header edits are visible to later tests and to messages stored
  after them, not before.

  ## Tests

    * `%{"header" => name, op => value}`, op one of `"contains"`, `"is"`, `"matches"`
      (glob `*` `?`), `"regex"`, `"contains_any"` (list), `"exists" => true`; every
      occurrence of the header is tried, RFC 2047 decoded;
    * `%{"body" => %{op => value}}`: any text part, decoded (`Core.MimeText`), with
      `"contains"`, `"contains_any"`, `"regex"` or `"regex_any"`;
    * `"case" => "sensitive"` (Sieve `i;octet`); the default is ASCII case-insensitive
      (Sieve's `i;ascii-casemap`);
    * `%{"any" => [...]}`, `%{"all" => [...]}`, `%{"not" => test}`, `%{"class" => [..]}`,
      `true`;
    * a header `regex` with `"capture" => true` binds its groups as `${1}`, `${2}`, ...
      for the tests after it in the same `all`; there they are inserted regex-escaped.

  Regexes are PCRE with `$` matching only at the end (POSIX, as in Pigeonhole), run
  with a match limit: a pathological pattern counts as no match instead of hanging.

  ## Actions

  `"keep"`, `"discard"`, `"stop"`, `"forward"` or `%{"forward" => %{"headers" =>
  [[name, value]]}}` (the headers go on the forwarded copies only),
  `%{"redirect" => address}`, `%{"add_header" => [name, value]}` (on top, as Sieve
  `addheader`), `%{"set_header" => [name, value]}`, `%{"delete_header" => name}`,
  `%{"set_class" => class}`, `%{"notify" => name}` (reported to the caller, e.g. CRM
  hooks). Values may use `${from_address}`, `${to_address}`, `${message_id}`.
  """
  alias Mailstore.Core.{Headers, MimeText}

  @enforce_keys [:name, :stage, :position, :test, :actions]
  defstruct [:id, :name, :stage, :position, :test, :actions, :scope]

  @stages ~w(detect forward)
  @classes ~w(other warmup reply bounce sent spam_test)
  @run_opts [match_limit: 1_000_000, match_limit_recursion: 50_000]

  @type outcome :: %{
          keep: binary | nil,
          discarded: boolean,
          copies: [{target :: term, binary}],
          class: atom,
          matched: [String.t()],
          notifications: [{String.t(), map}]
        }

  ## Compiling

  @doc "Validate and compile a rule map. `{:ok, %Rules{}}` or `{:error, reason}`."
  def compile(%{} = rule) do
    with {:ok, name} <- fetch(rule, "name", &is_binary/1),
         {:ok, stage} <- fetch(rule, "stage", &(&1 in @stages)),
         {:ok, test} <- compile_test(Map.get(rule, "test", true)),
         {:ok, actions} <- compile_actions(Map.get(rule, "actions")) do
      {:ok,
       %__MODULE__{
         id: rule["id"],
         name: name,
         stage: String.to_existing_atom(stage),
         position: rule["position"] || 0,
         scope: rule["scope"],
         test: test,
         actions: actions
       }}
    end
  end

  defp fetch(map, key, valid?) do
    value = Map.get(map, key)
    if valid?.(value), do: {:ok, value}, else: {:error, "#{key}: invalid #{inspect(value)}"}
  end

  defp compile_test(true), do: {:ok, true}
  defp compile_test(nil), do: {:ok, true}

  defp compile_test(%{"any" => tests}) when is_list(tests), do: compile_list(tests, &{:any, &1})
  defp compile_test(%{"all" => tests}) when is_list(tests), do: compile_list(tests, &{:all, &1})

  defp compile_test(%{"not" => test}) do
    with {:ok, t} <- compile_test(test), do: {:ok, {:not, t}}
  end

  defp compile_test(%{"class" => classes}) do
    classes = List.wrap(classes)

    if Enum.all?(classes, &(&1 in @classes)),
      do: {:ok, {:class, Enum.map(classes, &String.to_existing_atom/1)}},
      else: {:error, "class: unknown #{inspect(classes)}"}
  end

  defp compile_test(%{"header" => name} = t) when is_binary(name) do
    with {:ok, match} <- compile_match(Map.drop(t, ["header"])),
         do: {:ok, {:header, String.downcase(name), match}}
  end

  defp compile_test(%{"body" => %{} = t}) do
    with {:ok, match} <- compile_match(t), do: {:ok, {:body, match}}
  end

  defp compile_test(other), do: {:error, "unknown test #{inspect(other)}"}

  defp compile_list(tests, wrap) do
    tests
    |> Enum.reduce_while({:ok, []}, fn t, {:ok, acc} ->
      case compile_test(t) do
        {:ok, c} -> {:cont, {:ok, [c | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, wrap.(Enum.reverse(acc))}
      error -> error
    end
  end

  # {op, compiled, case_sensitive?, capture?}
  defp compile_match(t) do
    sensitive = Map.get(t, "case") == "sensitive"
    capture = Map.get(t, "capture") == true
    ops = Map.drop(t, ["case", "capture"])

    case Map.to_list(ops) do
      [{"exists", true}] ->
        {:ok, {:exists, nil, sensitive, false}}

      [{"contains", s}] when is_binary(s) ->
        {:ok, {:contains, [fold(s, sensitive)], sensitive, false}}

      [{"contains_any", l}] when is_list(l) ->
        {:ok, {:contains, Enum.map(l, &fold(&1, sensitive)), sensitive, false}}

      [{"is", s}] when is_binary(s) ->
        {:ok, {:is, fold(s, sensitive), sensitive, false}}

      [{"matches", glob}] when is_binary(glob) ->
        regexes([glob_to_regex(glob)], sensitive, capture)

      [{"regex", re}] when is_binary(re) ->
        regexes([re], sensitive, capture)

      [{"regex_any", res}] when is_list(res) ->
        regexes(res, sensitive, capture)

      _ ->
        {:error,
         "expected one of contains, contains_any, is, matches, regex, regex_any, exists in #{inspect(t)}"}
    end
  end

  # A pattern with ${n} is compiled when evaluated, after the captures are known.
  defp regexes(patterns, sensitive, capture) do
    patterns
    |> Enum.reduce_while({:ok, []}, fn p, {:ok, acc} ->
      if Regex.match?(~r/\$\{\d+\}/, p) do
        {:cont, {:ok, [{:dynamic, p} | acc]}}
      else
        case :re.compile(p, re_opts(sensitive)) do
          {:ok, mp} -> {:cont, {:ok, [mp | acc]}}
          {:error, {msg, pos}} -> {:halt, {:error, "regex #{inspect(p)} at #{pos}: #{msg}"}}
        end
      end
    end)
    |> case do
      {:ok, compiled} -> {:ok, {:regex, Enum.reverse(compiled), sensitive, capture}}
      error -> error
    end
  end

  defp re_opts(true), do: [:dollar_endonly]
  defp re_opts(false), do: [:dollar_endonly, :caseless]

  defp glob_to_regex(glob) do
    body =
      glob
      |> String.graphemes()
      |> Enum.map_join(fn
        "*" -> ".*"
        "?" -> "."
        c -> Regex.escape(c)
      end)

    "\\A" <> body <> "\\z"
  end

  defp compile_actions(actions) when is_list(actions) and actions != [] do
    Enum.reduce_while(actions, {:ok, []}, fn a, {:ok, acc} ->
      case compile_action(a) do
        {:ok, c} -> {:cont, {:ok, acc ++ [c]}}
        error -> {:halt, error}
      end
    end)
  end

  defp compile_actions(other),
    do: {:error, "actions: expected a non-empty list, got #{inspect(other)}"}

  defp compile_action(a) when a in ["keep", "discard", "stop"],
    do: {:ok, String.to_existing_atom(a)}

  defp compile_action("forward"), do: {:ok, {:forward, []}}

  defp compile_action(%{"forward" => %{} = opts}) do
    headers = Map.get(opts, "headers", [])

    if Enum.all?(headers, &header_pair?/1),
      do: {:ok, {:forward, Enum.map(headers, &List.to_tuple/1)}},
      else: {:error, "forward headers: expected [[name, value], ...]"}
  end

  defp compile_action(%{"redirect" => address}) when is_binary(address),
    do: {:ok, {:redirect, address}}

  defp compile_action(%{"delete_header" => name}) when is_binary(name),
    do: {:ok, {:delete_header, name}}

  defp compile_action(%{"notify" => name}) when is_binary(name), do: {:ok, {:notify, name}}

  defp compile_action(%{"set_class" => class}) when class in @classes,
    do: {:ok, {:set_class, String.to_existing_atom(class)}}

  defp compile_action(%{"add_header" => pair}) do
    if header_pair?(pair),
      do: {:ok, {:add_header, List.to_tuple(pair)}},
      else: {:error, "add_header: expected [name, value]"}
  end

  defp compile_action(%{"set_header" => pair}) do
    if header_pair?(pair),
      do: {:ok, {:set_header, List.to_tuple(pair)}},
      else: {:error, "set_header: expected [name, value]"}
  end

  defp compile_action(other), do: {:error, "unknown action #{inspect(other)}"}

  defp header_pair?([name, value]) when is_binary(name) and is_binary(value),
    do: Regex.match?(~r/^[!-9;-~]+$/, name) and not String.contains?(value, ["\r", "\n"])

  defp header_pair?(_), do: false

  ## Evaluating

  @doc """
  Run `rules` (compiled, any order: they are sorted by stage and position) on a raw
  message. `opts`: `:class` (from upstream, default `:other`), `:to_address` (envelope
  recipient), `:forwardings` (`[%{target: term, mode: "copy" | "redirect"}]`, already
  filtered by class), `:tree` (the message's `Mime.parse/1` result, if already known).

  Returns the outcome: `keep` (the bytes to store in the recipient's mailbox, or nil),
  `discarded` (a `discard` action ran),
  `copies` (`{target, bytes}` for forwardings and redirects, `{:address, addr}` for
  redirects), the final `class`, the names of the rules that matched, notifications.
  """
  @spec evaluate([%__MODULE__{}], binary, keyword) :: outcome
  def evaluate(rules, raw, opts \\ []) do
    forwardings = Keyword.get(opts, :forwardings, [])
    headers = Headers.parse(raw)

    st = %{
      raw: raw,
      orig: raw,
      tree: Keyword.get(opts, :tree),
      headers: headers,
      parts: nil,
      folded_parts: nil,
      decoded: %{},
      captures: [],
      class: Keyword.get(opts, :class, :other),
      vars: vars(headers, Keyword.get(opts, :to_address)),
      forwardings: forwardings,
      keep: nil,
      implicit_keep: true,
      discarded: false,
      forwarded: false,
      copies: [],
      matched: [],
      notifications: [],
      stopped: false
    }

    {detect, forward} =
      rules |> Enum.sort_by(&{&1.position, &1.id || 0}) |> Enum.split_with(&(&1.stage == :detect))

    st = run_stage(st, detect)

    st =
      if not st.stopped and forwardings != [] do
        st = run_stage(st, forward)
        if st.stopped or st.forwarded, do: st, else: do_forward(st, [])
      else
        st
      end

    keep = st.keep || if(st.implicit_keep, do: st.raw)

    %{
      keep: keep,
      discarded: st.discarded,
      copies: Enum.reverse(st.copies),
      class: st.class,
      matched: Enum.reverse(st.matched),
      notifications: Enum.reverse(st.notifications)
    }
  end

  defp run_stage(st, []), do: st
  defp run_stage(%{stopped: true} = st, _rules), do: st

  defp run_stage(st, [rule | rest]) do
    {hit, st} = test(rule.test, %{st | captures: []})

    st =
      if hit, do: run_actions(%{st | matched: [rule.name | st.matched]}, rule.actions), else: st

    run_stage(st, rest)
  end

  defp run_actions(st, []), do: st
  defp run_actions(st, [:stop | _]), do: %{st | stopped: true}
  defp run_actions(st, [action | rest]), do: run_actions(act(action, st), rest)

  defp act(:keep, st), do: %{st | keep: st.raw, implicit_keep: false}
  defp act(:discard, st), do: %{st | implicit_keep: false, discarded: true}
  defp act({:forward, headers}, st), do: do_forward(st, headers)

  defp act({:redirect, address}, st),
    do: %{
      st
      | copies: [{{:address, expand(address, st)}, st.raw} | st.copies],
        implicit_keep: false
    }

  defp act({:set_class, class}, st), do: %{st | class: class}
  defp act({:notify, name}, st), do: %{st | notifications: [{name, st.vars} | st.notifications]}

  defp act({:add_header, {name, value}}, st), do: put_header(st, name, expand(value, st))
  defp act({:delete_header, name}, st), do: remove_header(st, name)

  defp act({:set_header, {name, value}}, st),
    do: st |> remove_header(name) |> put_header(name, expand(value, st))

  defp do_forward(%{forwarded: true} = st, _headers), do: st

  defp do_forward(st, headers) do
    copy = Enum.reduce(headers, st.raw, fn {n, v}, raw -> add_header(raw, n, expand(v, st)) end)
    copies = Enum.reduce(st.forwardings, st.copies, &[{&1.target, copy} | &2])
    redirect? = Enum.any?(st.forwardings, &(&1.mode == "redirect"))

    %{st | copies: copies, forwarded: true, implicit_keep: st.implicit_keep and not redirect?}
  end

  # The parsed header list is kept in step with the bytes instead of being parsed again.
  defp put_header(st, name, value),
    do: %{
      st
      | raw: add_header(st.raw, name, value),
        headers: [{String.downcase(name), value} | st.headers],
        decoded: %{}
    }

  defp remove_header(st, name) do
    lname = String.downcase(name)

    %{
      st
      | raw: delete_header(st.raw, name),
        headers: Enum.reject(st.headers, &(elem(&1, 0) == lname)),
        decoded: %{}
    }
  end

  ## Tests

  defp test(true, st), do: {true, st}
  defp test({:not, t}, st), do: with({hit, st} <- test(t, st), do: {not hit, st})
  defp test({:class, classes}, st), do: {st.class in classes, st}

  defp test({:any, tests}, st) do
    Enum.reduce_while(tests, {false, st}, fn t, {false, st} ->
      case test(t, st) do
        {true, st} -> {:halt, {true, st}}
        {false, st} -> {:cont, {false, st}}
      end
    end)
  end

  defp test({:all, tests}, st) do
    Enum.reduce_while(tests, {true, st}, fn t, {true, st} ->
      case test(t, st) do
        {true, st} -> {:cont, {true, st}}
        {false, st} -> {:halt, {false, st}}
      end
    end)
  end

  # Decoded values per header name, computed once per message (header edits reset it).
  defp test({:header, name, match}, st) do
    case st.decoded do
      %{^name => values} ->
        match_any(values, match, st)

      _ ->
        values = for {^name, v} <- st.headers, do: MimeText.decode_header(v)
        match_any(values, match, %{st | decoded: Map.put(st.decoded, name, values)})
    end
  end

  defp test({:body, match}, st) do
    # Header edits never touch the body: parts come from the original once, and their
    # case-folded copies once, for the case-insensitive tests.
    st = if st.parts, do: st, else: %{st | parts: MimeText.parts(st.orig, st.tree)}

    case match do
      {op, _, false, _} when op in [:contains, :is] ->
        st =
          if st.folded_parts,
            do: st,
            else: %{st | folded_parts: Enum.map(st.parts, &fold(&1, false))}

        match_any(st.folded_parts, put_elem(match, 2, true), st)

      _ ->
        match_any(st.parts, match, st)
    end
  end

  defp match_any([], {:exists, _, _, _}, st), do: {false, st}
  defp match_any([_ | _], {:exists, _, _, _}, st), do: {true, st}

  defp match_any(values, {:contains, needles, sensitive, _}, st) do
    hit =
      Enum.any?(values, fn v ->
        v = fold(v, sensitive)
        Enum.any?(needles, &(:binary.match(v, &1) != :nomatch))
      end)

    {hit, st}
  end

  defp match_any(values, {:is, s, sensitive, _}, st),
    do: {Enum.any?(values, &(fold(&1, sensitive) == s)), st}

  defp match_any(values, {:regex, patterns, sensitive, capture}, st) do
    patterns = Enum.map(patterns, &resolve(&1, sensitive, st.captures))

    Enum.find_value(values, {false, st}, fn v ->
      Enum.find_value(patterns, fn
        nil ->
          nil

        mp ->
          case :re.run(v, mp, [{:capture, :all_but_first, :binary} | @run_opts]) do
            {:match, groups} -> {true, if(capture, do: %{st | captures: groups}, else: st)}
            _nomatch_or_limit -> nil
          end
      end)
    end)
  end

  # ${n} in a pattern: the n-th capture of an earlier test, escaped.
  defp resolve({:dynamic, pattern}, sensitive, captures) do
    expanded =
      Regex.replace(~r/\$\{(\d+)\}/, pattern, fn _, n ->
        captures |> Enum.at(String.to_integer(n) - 1, "") |> Regex.escape()
      end)

    case :re.compile(expanded, re_opts(sensitive)) do
      {:ok, mp} -> mp
      _ -> nil
    end
  end

  defp resolve(mp, _sensitive, _captures), do: mp

  ## Helpers

  defp fold(s, true), do: s
  defp fold(s, false), do: for(<<c <- s>>, into: "", do: <<if(c in ?A..?Z, do: c + 32, else: c)>>)

  # What the Sieve script set as variables: From address, envelope recipient, Message-ID.
  defp vars(headers, to_address) do
    from = Headers.get(headers, "from")

    from_address =
      case from && Regex.run(~r/<(.+@.+)>$/, from) do
        [_, addr] -> addr
        _ -> from || "no_address_found"
      end

    %{
      "from_address" => from_address,
      "to_address" => to_address || "",
      "message_id" => Headers.get(headers, "message-id") || "no_message_id_found"
    }
  end

  defp expand(value, st) do
    Regex.replace(~r/\$\{(from_address|to_address|message_id)\}/, value, fn _, v -> st.vars[v] end)
  end

  defp add_header(raw, name, value) do
    eol = if :binary.match(raw, "\r\n") != :nomatch, do: "\r\n", else: "\n"
    name <> ": " <> value <> eol <> raw
  end

  # Every occurrence, folded continuation lines included.
  defp delete_header(raw, name) do
    {block, body, sep} =
      case :binary.match(raw, ["\r\n\r\n", "\n\n"]) do
        {pos, len} ->
          {binary_part(raw, 0, pos), binary_part(raw, pos + len, byte_size(raw) - pos - len),
           binary_part(raw, pos, len)}

        :nomatch ->
          {raw, "", ""}
      end

    lname = String.downcase(name) <> ":"

    eol =
      if String.contains?(sep, "\r\n") or String.contains?(block, "\r\n"), do: "\r\n", else: "\n"

    {lines, _drop} =
      block
      |> String.split(~r/\r?\n/)
      |> Enum.reduce({[], false}, fn line, {acc, dropping} ->
        cond do
          dropping and String.starts_with?(line, [" ", "\t"]) -> {acc, true}
          String.starts_with?(String.downcase(line), lname) -> {acc, true}
          true -> {[line | acc], false}
        end
      end)

    Enum.join(Enum.reverse(lines), eol) <> sep <> body
  end
end

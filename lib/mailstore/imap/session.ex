defmodule Mailstore.Imap.Session do
  @moduledoc """
  IMAP4rev1 session state machine, independent of sockets.

  `handle_command/2` takes one complete command (from `Mailstore.Imap.Framing`) and
  returns the bytes to send. The connection process owns the socket; this module only
  calls the `Mailbox` and `Directory` contexts. Tests drive it directly.

  Live updates: when a folder is selected the session subscribes to the account actor,
  which sends `{:mailstore, folder_id, event}` to the connection process. Events are
  queued and turned into untagged responses only where IMAP allows it:

    * at the end of a command (before the tagged reply), and immediately during IDLE;
    * never EXPUNGE during FETCH / STORE / SEARCH without UID (sequence numbers would
      shift under the client), so those keep EXPUNGE queued.

  The actor also reports this session's own changes. They are harmless to apply twice
  (EXISTS/EXPUNGE are idempotent on the UID map); flag pushes for our own STORE are
  dropped by MODSEQ.
  """
  require Logger

  alias Mailstore.{Directory, Mailbox}
  alias Mailstore.Core.{Flags, NewMessage}
  alias Mailstore.Imap.{Fetch, Format, Parser, Search, SequenceSet, UidMap}

  @capabilities "IMAP4rev1 LITERAL+ IDLE UIDPLUS MOVE NAMESPACE ID ENABLE UNSELECT SPECIAL-USE CHILDREN"
  @system_flags "(\\Answered \\Flagged \\Deleted \\Seen \\Draft)"
  @permanent_flags "(\\Answered \\Flagged \\Deleted \\Seen \\Draft \\*)"

  @any ~w(CAPABILITY NOOP LOGOUT ID)
  @not_authenticated ~w(LOGIN AUTHENTICATE STARTTLS)
  @authenticated ~w(SELECT EXAMINE CREATE DELETE RENAME SUBSCRIBE UNSUBSCRIBE LIST LSUB STATUS APPEND NAMESPACE ENABLE IDLE)
  @selected ["CHECK", "CLOSE", "UNSELECT", "EXPUNGE", "SEARCH", "FETCH", "STORE", "COPY", "MOVE"] ++
              ["UID SEARCH", "UID FETCH", "UID STORE", "UID COPY", "UID MOVE", "UID EXPUNGE"]
  # Commands during which EXPUNGE must not be sent.
  @no_expunge ~w(FETCH STORE SEARCH)

  defstruct state: :not_authenticated,
            account: nil,
            folder: nil,
            map: %UidMap{},
            actor: nil,
            highest_modseq: 0,
            own_modseqs: MapSet.new(),
            pending: [],
            idle_tag: nil

  @type t :: %__MODULE__{}

  def new, do: %__MODULE__{}
  def greeting, do: "* OK [CAPABILITY #{@capabilities}] mailstore ready\r\n"
  def logout?(%__MODULE__{state: state}), do: state == :logout
  def idle?(%__MODULE__{idle_tag: tag}), do: tag != nil

  ## Commands

  @spec handle_command(binary, t) :: {iodata, t}
  def handle_command(line, %__MODULE__{idle_tag: tag} = s) when tag != nil do
    if line |> String.trim() |> String.upcase() == "DONE" do
      {out, s} = flush(%{s | idle_tag: nil}, :all)
      {[out, tag, " OK IDLE terminated\r\n"], s}
    else
      {"* BAD Expected DONE\r\n", s}
    end
  end

  def handle_command(command, s) do
    case Parser.parse(command) do
      {:ok, tag, cmd, args} ->
        {untagged, result, s} =
          :telemetry.span([:mailstore, :imap, :command], %{command: cmd}, fn ->
            {untagged, result, s} = run(cmd, args, s)
            status = if result == :idle, do: :idle, else: elem(result, 0)
            {{untagged, result, s}, %{command: cmd, status: status}}
          end)

        case result do
          :idle ->
            {out, s} = flush(s, :all)
            {[untagged, out, "+ idling\r\n"], %{s | idle_tag: tag}}

          {status, text} ->
            {out, s} = flush(s, if(cmd in @no_expunge, do: :no_expunge, else: :all))
            {[untagged, out, tag, " ", status(status), " ", text, "\r\n"], s}
        end

      {:error, tag, reason} ->
        {[tag || "*", " BAD ", reason, "\r\n"], s}
    end
  end

  defp status(:ok), do: "OK"
  defp status(:no), do: "NO"
  defp status(:bad), do: "BAD"

  defp run(cmd, args, s) do
    cond do
      cmd in @any ->
        command(cmd, args, s)

      cmd in @not_authenticated and s.state == :not_authenticated ->
        command(cmd, args, s)

      cmd in @authenticated and s.state in [:authenticated, :selected] ->
        command(cmd, args, s)

      cmd in @selected and s.state == :selected ->
        command(cmd, args, s)

      cmd in @not_authenticated ->
        {[], {:bad, "Already authenticated"}, s}

      cmd in @authenticated or cmd in @selected ->
        {[], {:bad, "Command not allowed in this state"}, s}

      true ->
        {[], {:bad, "Unknown command"}, s}
    end
  rescue
    e ->
      Logger.error("IMAP #{cmd} failed: " <> Exception.format(:error, e, __STACKTRACE__))
      {[], {:no, "[SERVERBUG] Internal error"}, s}
  end

  # -- any state --

  defp command("CAPABILITY", _, s),
    do: {["* CAPABILITY ", @capabilities, "\r\n"], {:ok, "CAPABILITY completed"}, s}

  defp command("NOOP", _, s), do: {[], {:ok, "NOOP completed"}, s}
  defp command("ID", _, s), do: {[~s{* ID ("name" "mailstore")\r\n}], {:ok, "ID completed"}, s}

  defp command("LOGOUT", _, s),
    do: {["* BYE Logging out\r\n"], {:ok, "LOGOUT completed"}, %{unselect(s) | state: :logout}}

  # -- not authenticated --

  defp command("LOGIN", [user, pass], s) do
    with {:ok, user} <- astring(user),
         {:ok, pass} <- astring(pass),
         {:ok, account} <- Directory.authenticate(user, pass) do
      account = %{id: account.id, tenant_id: account.tenant_id, address: account.address}

      {[], {:ok, "[CAPABILITY #{@capabilities}] LOGIN completed"},
       %{s | state: :authenticated, account: account}}
    else
      {:error, :invalid_credentials} ->
        {[], {:no, "[AUTHENTICATIONFAILED] Invalid credentials"}, s}

      _ ->
        {[], {:bad, "Invalid arguments"}, s}
    end
  end

  defp command("LOGIN", _, s), do: {[], {:bad, "LOGIN needs a user name and a password"}, s}
  defp command("AUTHENTICATE", _, s), do: {[], {:no, "Use LOGIN"}, s}
  defp command("STARTTLS", _, s), do: {[], {:no, "TLS is terminated by the load balancer"}, s}

  # -- authenticated --

  defp command("NAMESPACE", _, s),
    do: {[~s{* NAMESPACE (("" "/")) NIL NIL\r\n}], {:ok, "NAMESPACE completed"}, s}

  defp command("ENABLE", _, s), do: {["* ENABLED\r\n"], {:ok, "ENABLE completed"}, s}
  defp command("IDLE", _, s), do: {[], :idle, s}

  defp command(cmd, _, s) when cmd in ["SUBSCRIBE", "UNSUBSCRIBE"],
    do: {[], {:ok, "#{cmd} completed"}, s}

  defp command(cmd, _, s) when cmd in ["CREATE", "DELETE", "RENAME"],
    do: {[], {:no, "Folder management is not implemented yet"}, s}

  defp command(cmd, [name | _], s) when cmd in ["SELECT", "EXAMINE"] do
    case astring(name) do
      {:ok, name} -> select(s, name, cmd)
      :error -> {[], {:bad, "Invalid folder name"}, s}
    end
  end

  defp command(cmd, [ref, pattern], s) when cmd in ["LIST", "LSUB"] do
    with {:ok, ref} <- astring(ref), {:ok, pattern} <- astring(pattern) do
      {list(s, cmd, ref <> pattern, pattern), {:ok, "#{cmd} completed"}, s}
    else
      _ -> {[], {:bad, "Invalid arguments"}, s}
    end
  end

  defp command("STATUS", [name, {:list, items}], s) do
    with {:ok, name} <- astring(name),
         {:ok, view} <- Mailbox.status(s.account.id, name) do
      values =
        for {:atom, item} <- items,
            value = status_item(String.upcase(item), view),
            value != nil,
            do: value

      {["* STATUS ", Format.string(view.name), " (", Enum.join(values, " "), ")\r\n"],
       {:ok, "STATUS completed"}, s}
    else
      {:error, :no_such_folder} -> {[], {:no, "No such folder"}, s}
      _ -> {[], {:bad, "Invalid arguments"}, s}
    end
  end

  defp command("APPEND", [name | rest], s) do
    with {:ok, name} <- astring(name),
         {:ok, flags, date, raw} <- append_args(rest),
         msg = NewMessage.from_raw(raw, flags: flags, internal_date: date),
         {:ok, [uid]} <- Mailbox.append(s.account.id, name, [msg]),
         {:ok, view} <- Mailbox.status(s.account.id, name) do
      {[], {:ok, "[APPENDUID #{view.uidvalidity} #{uid}] APPEND completed"}, s}
    else
      {:error, :no_such_folder} -> {[], {:no, "[TRYCREATE] No such folder"}, s}
      {:error, reason} when is_binary(reason) -> {[], {:bad, reason}, s}
      _ -> {[], {:bad, "Invalid arguments"}, s}
    end
  end

  # -- selected --

  defp command("CHECK", _, s), do: {[], {:ok, "CHECK completed"}, s}
  defp command("UNSELECT", _, s), do: {[], {:ok, "UNSELECT completed"}, unselect(s)}

  defp command("CLOSE", _, s) do
    unless s.folder.read_only, do: Mailbox.expunge(s.account.id, s.folder.name, :all)
    {[], {:ok, "CLOSE completed"}, unselect(s)}
  end

  defp command("EXPUNGE", _, s), do: expunge(s, :all)

  defp command("UID EXPUNGE", [set], s) do
    case resolve(s, set, true) do
      {:ok, uids} -> expunge(s, uids)
      _ -> bad_set(s)
    end
  end

  defp command(cmd, [set | items], s) when cmd in ["FETCH", "UID FETCH"] do
    uid? = cmd == "UID FETCH"

    with {:ok, uids} <- resolve(s, set, uid?),
         {:ok, items} <- Fetch.parse(items) do
      items = if uid? and :uid not in items, do: [:uid | items], else: items
      {out, s} = fetch(s, uids, items)
      {out, {:ok, "FETCH completed"}, s}
    else
      {:error, reason} -> {[], {:bad, reason}, s}
      _ -> bad_set(s)
    end
  end

  defp command(cmd, [set, {:atom, op} | flags], s) when cmd in ["STORE", "UID STORE"] do
    uid? = cmd == "UID STORE"

    with {:ok, uids} <- resolve(s, set, uid?),
         {:ok, mode, silent?} <- store_op(op),
         {:ok, flags} <- flag_args(flags) do
      store(s, uids, mode, silent?, flags, uid?)
    else
      {:error, reason} -> {[], {:bad, reason}, s}
      _ -> bad_set(s)
    end
  end

  defp command(cmd, args, s) when cmd in ["SEARCH", "UID SEARCH"] do
    uid? = cmd == "UID SEARCH"

    case Search.parse(args) do
      {:ok, tree} ->
        # Apply queued pushes first: mail that just arrived is in the result and its
        # EXISTS goes out before the SEARCH response (no EXPUNGE during plain SEARCH).
        {pre, s} = flush(s, if(uid?, do: :all, else: :no_expunge))
        {[pre, search(s, tree, uid?)], {:ok, "SEARCH completed"}, s}

      {:error, :badcharset} ->
        {[], {:no, "[BADCHARSET (UTF-8 US-ASCII)] Unsupported charset"}, s}

      {:error, reason} ->
        {[], {:bad, reason}, s}
    end
  end

  defp command(cmd, [set, dest], s) when cmd in ["COPY", "UID COPY", "MOVE", "UID MOVE"] do
    uid? = String.starts_with?(cmd, "UID ")
    move? = String.ends_with?(cmd, "MOVE")

    with {:ok, uids} <- resolve(s, set, uid?),
         {:ok, dest} <- astring(dest) do
      if move?, do: move(s, uids, dest), else: copy(s, uids, dest)
    else
      _ -> bad_set(s)
    end
  end

  defp command(cmd, _, s), do: {[], {:bad, "Invalid arguments for #{cmd}"}, s}

  ## Command implementations

  defp select(s, name, cmd) do
    s = unselect(s)
    read_only = cmd == "EXAMINE"

    case Mailbox.subscribe(s.account.id, name) do
      {:ok, actor, view} ->
        # Subscribe first, then load: anything that lands in between arrives as a push and
        # is ignored if already loaded.
        map = UidMap.from_ranges(Mailbox.uid_ranges(s.account.tenant_id, view.id))

        unseen =
          with uid when is_integer(uid) <- Mailbox.first_unseen_uid(s.account.tenant_id, view.id),
               seq when is_integer(seq) <- UidMap.seq(map, uid) do
            ["* OK [UNSEEN ", Integer.to_string(seq), "] First unseen\r\n"]
          else
            _ -> []
          end

        s = %{
          s
          | state: :selected,
            folder: %{
              id: view.id,
              name: view.name,
              uidvalidity: view.uidvalidity,
              read_only: read_only
            },
            map: map,
            actor: actor,
            highest_modseq: view.highest_modseq
        }

        out = [
          ["* FLAGS ", @system_flags, "\r\n"],
          [
            "* OK [PERMANENTFLAGS ",
            if(read_only, do: "()", else: @permanent_flags),
            "] Flags permitted\r\n"
          ],
          ["* ", Integer.to_string(UidMap.size(map)), " EXISTS\r\n"],
          "* 0 RECENT\r\n",
          unseen,
          ["* OK [UIDVALIDITY ", Integer.to_string(view.uidvalidity), "] UIDs valid\r\n"],
          [
            "* OK [UIDNEXT ",
            Integer.to_string(max(view.uidnext, UidMap.max_uid(map) + 1)),
            "] Predicted next UID\r\n"
          ]
        ]

        {out, {:ok, "[#{if read_only, do: "READ-ONLY", else: "READ-WRITE"}] #{cmd} completed"}, s}

      {:error, :no_such_folder} ->
        {[], {:no, "No such folder"}, s}
    end
  end

  defp unselect(%__MODULE__{state: :selected} = s) do
    Mailbox.unsubscribe(s.account.id)

    %{
      s
      | state: :authenticated,
        folder: nil,
        map: %UidMap{},
        actor: nil,
        pending: [],
        own_modseqs: MapSet.new()
    }
  end

  defp unselect(s), do: s

  defp list(s, cmd, full_pattern, pattern) do
    if pattern == "" do
      [~s{* }, cmd, ~s{ (\\Noselect) "/" ""\r\n}]
    else
      regex = pattern_regex(full_pattern)

      for f <- Mailbox.list_folders(s.account.tenant_id, s.account.id),
          Regex.match?(regex, f.name) or (f.name == "INBOX" and Regex.match?(regex, "inbox")) do
        attrs = Enum.reject(["\\HasNoChildren", f.special_use], &is_nil/1)
        ["* ", cmd, " (", Enum.join(attrs, " "), ~s{) "/" }, Format.string(f.name), "\r\n"]
      end
    end
  end

  defp pattern_regex(pattern) do
    body =
      pattern
      |> Regex.escape()
      |> String.replace("\\*", ".*")
      |> String.replace("%", "[^/]*")

    Regex.compile!("^" <> body <> "$", if(String.upcase(pattern) == "INBOX", do: "i", else: ""))
  end

  defp status_item("MESSAGES", v), do: "MESSAGES #{v.messages_count}"
  defp status_item("UIDNEXT", v), do: "UIDNEXT #{v.uidnext}"
  defp status_item("UIDVALIDITY", v), do: "UIDVALIDITY #{v.uidvalidity}"
  defp status_item("UNSEEN", v), do: "UNSEEN #{v.unseen_count}"
  defp status_item("RECENT", _v), do: "RECENT 0"
  defp status_item(_, _v), do: nil

  defp append_args(tokens) do
    case Enum.reverse(tokens) do
      [{:string, raw} | opts] ->
        Enum.reduce_while(Enum.reverse(opts), {:ok, [], nil, raw}, fn
          {:list, flags}, {:ok, _, date, raw} ->
            {:cont, {:ok, for({:atom, f} <- flags, do: f), date, raw}}

          {:string, date}, {:ok, flags, _, raw} ->
            case Format.parse_datetime(date) do
              {:ok, dt} -> {:cont, {:ok, flags, dt, raw}}
              :error -> {:halt, {:error, "Invalid date-time"}}
            end

          _, _ ->
            {:halt, {:error, "Invalid APPEND arguments"}}
        end)

      _ ->
        {:error, "APPEND needs a message literal"}
    end
  end

  defp fetch(s, uids, items) do
    %{account: account, folder: folder} = s
    structure? = Fetch.needs_structure?(items)
    msgs = Mailbox.fetch_meta(account.tenant_id, folder.id, uids, structure?)

    # A non-PEEK body fetch sets \Seen; report the new flags in the same response.
    {msgs, seen_now, s} =
      if Fetch.sets_seen?(items) and not folder.read_only do
        to_mark = for m <- msgs, "\\Seen" not in m.flags, do: m.uid

        if to_mark == [] do
          {msgs, MapSet.new(), s}
        else
          {:ok, changes} = Mailbox.store_flags(account.id, folder.name, to_mark, :add, ["\\Seen"])
          new_flags = Map.new(changes, &{&1.uid, &1.flags})
          msgs = Enum.map(msgs, fn m -> %{m | flags: Map.get(new_flags, m.uid, m.flags)} end)
          {msgs, MapSet.new(Map.keys(new_flags)), own(s, changes)}
        end
      else
        {msgs, MapSet.new(), s}
      end

    # Bodies for sections; rows stored before MIME parsing get their tree from the body.
    raw_uids =
      cond do
        Fetch.needs_body?(items) -> uids
        structure? -> for m <- msgs, m.bodystructure == nil, do: m.uid
        true -> []
      end

    raws = Mailbox.fetch_raw_many(account.tenant_id, folder.id, raw_uids)

    out =
      for m <- msgs, seq = UidMap.seq(s.map, m.uid), seq != nil do
        items =
          if MapSet.member?(seen_now, m.uid) and :flags not in items,
            do: items ++ [:flags],
            else: items

        Fetch.render(seq, items, m, raws[m.uid])
      end

    {out, s}
  end

  defp store(%{folder: %{read_only: true}} = s, _, _, _, _, _),
    do: {[], {:no, "Folder is read-only"}, s}

  defp store(s, uids, mode, silent?, flags, uid?) do
    {:ok, changes} = Mailbox.store_flags(s.account.id, s.folder.name, uids, mode, flags)
    s = own(s, changes)

    out =
      if silent? do
        []
      else
        for m <- Mailbox.fetch_meta(s.account.tenant_id, s.folder.id, uids),
            seq = UidMap.seq(s.map, m.uid),
            seq != nil do
          Fetch.render(seq, if(uid?, do: [:uid, :flags], else: [:flags]), m, nil)
        end
      end

    {out, {:ok, "STORE completed"}, s}
  end

  defp expunge(%{folder: %{read_only: true}} = s, _), do: {[], {:no, "Folder is read-only"}, s}

  defp expunge(s, uids) do
    {:ok, expunged} = Mailbox.expunge(s.account.id, s.folder.name, uids)
    {out, s} = remove(s, expunged)
    {out, {:ok, "EXPUNGE completed"}, s}
  end

  defp copy(s, uids, dest) do
    case Mailbox.copy(s.account.id, s.folder.name, uids, dest) do
      {:ok, %{mapping: []}} ->
        {[], {:ok, "COPY completed"}, s}

      {:ok, %{uidvalidity: v, mapping: m}} ->
        {[], {:ok, "[COPYUID #{v} #{copyuid(m)}] COPY completed"}, s}

      {:error, :no_such_folder} ->
        {[], {:no, "[TRYCREATE] No such folder"}, s}
    end
  end

  defp move(%{folder: %{read_only: true}} = s, _, _), do: {[], {:no, "Folder is read-only"}, s}

  defp move(s, uids, dest) do
    case Mailbox.move(s.account.id, s.folder.name, uids, dest) do
      {:ok, %{mapping: []}} ->
        {[], {:ok, "MOVE completed"}, s}

      {:ok, %{uidvalidity: v, mapping: m}} ->
        {expunges, s} = remove(s, Enum.map(m, &elem(&1, 0)))
        {[["* OK [COPYUID #{v} #{copyuid(m)}] Moved\r\n"], expunges], {:ok, "MOVE completed"}, s}

      {:error, :no_such_folder} ->
        {[], {:no, "[TRYCREATE] No such folder"}, s}
    end
  end

  defp copyuid(mapping) do
    {src, dst} = Enum.unzip(mapping)
    "#{SequenceSet.format(src)} #{SequenceSet.format(dst)}"
  end

  # Cheapest data first (see Search.needs/1):
  #   :ids   - UID/sequence sets, answered from the session's UID map, no database
  #   :flags - pure UNSEEN from the partial index; otherwise narrow flag rows
  #   :full  - envelope (and bodies for TEXT/BODY/non-envelope HEADER)
  defp search(s, tree, uid?) do
    %{account: account, folder: folder, map: map} = s
    ctx = %{max_seq: UidMap.size(map), max_uid: UidMap.max_uid(map)}
    candidates = Search.candidates(tree, map)

    hits =
      case Search.needs(tree) do
        :ids ->
          uids = if candidates == :all, do: UidMap.to_list(map), else: candidates
          msgs = for uid <- uids, do: %{uid: uid, seq: UidMap.seq(map, uid)}
          Search.run(tree, msgs, ctx)

        :flags ->
          if Search.only_unseen?(tree) do
            for uid <- Mailbox.unseen_uids(account.tenant_id, folder.id),
                seq = UidMap.seq(map, uid),
                seq != nil,
                do: %{uid: uid, seq: seq}
          else
            rows = Mailbox.fetch_flag_rows(account.tenant_id, folder.id, candidates)
            Search.run(tree, with_seq(rows, map, %{}), ctx)
          end

        :full ->
          uids = if candidates == :all, do: UidMap.to_list(map), else: candidates

          raws =
            if Search.needs_body?(tree),
              do: Mailbox.fetch_raw_many(account.tenant_id, folder.id, uids),
              else: %{}

          rows = Mailbox.fetch_meta(account.tenant_id, folder.id, uids)
          Search.run(tree, with_seq(rows, map, raws), ctx)
      end

    numbers = Enum.map(hits, &if(uid?, do: &1.uid, else: &1.seq))
    ["* SEARCH", Enum.map(numbers, &[" ", Integer.to_string(&1)]), "\r\n"]
  end

  # Keep only messages this session knows about, with their sequence numbers.
  defp with_seq(rows, map, raws) do
    for m <- rows, seq = UidMap.seq(map, m.uid), seq != nil do
      Map.merge(m, %{seq: seq, raw: raws[m.uid]})
    end
  end

  ## Arguments

  defp astring({:atom, a}), do: {:ok, a}
  defp astring({:string, s}), do: {:ok, s}
  defp astring(_), do: :error

  defp resolve(s, {:atom, raw}, uid?) do
    with {:ok, set} <- SequenceSet.parse(raw) do
      uids =
        if uid?,
          do: SequenceSet.uids(set, s.map),
          else: set |> SequenceSet.seqs(UidMap.size(s.map)) |> Enum.map(&UidMap.uid(s.map, &1))

      {:ok, uids}
    end
  end

  defp resolve(_s, _token, _uid?), do: :error

  defp bad_set(s), do: {[], {:bad, "Invalid sequence set"}, s}

  defp store_op(op) do
    up = String.upcase(op)
    {base, silent?} = {String.replace_suffix(up, ".SILENT", ""), String.ends_with?(up, ".SILENT")}

    case base do
      "+FLAGS" -> {:ok, :add, silent?}
      "-FLAGS" -> {:ok, :remove, silent?}
      "FLAGS" -> {:ok, :replace, silent?}
      _ -> {:error, "Invalid STORE operation"}
    end
  end

  defp flag_args([{:list, flags}]), do: flag_args(flags)

  defp flag_args(flags) do
    if Enum.all?(flags, &match?({:atom, _}, &1)),
      do: {:ok, for({:atom, f} <- flags, do: f)},
      else: {:error, "Invalid flags"}
  end

  ## Live updates

  @doc "An actor push arrived at the connection. Returns output to send now (only during IDLE)."
  @spec handle_push({:mailstore, integer, term}, t) :: {iodata, t}
  def handle_push(
        {:mailstore, folder_id, event},
        %{state: :selected, folder: %{id: folder_id}} = s
      ) do
    s = %{s | pending: s.pending ++ [event]}
    if idle?(s), do: flush(s, :all), else: {[], s}
  end

  def handle_push(_stale, s), do: {[], s}

  @doc """
  The account actor died (node failure, deploy). Subscribe to its replacement and
  resync from the database: whatever happened meanwhile is reported now.
  """
  @spec handle_actor_down(t) :: {iodata, t}
  def handle_actor_down(%{state: :selected} = s) do
    case Mailbox.subscribe(s.account.id, s.folder.name) do
      {:ok, actor, _view} -> resync(%{s | actor: actor})
      {:error, _} -> {"* BYE Folder is unavailable\r\n", %{s | state: :logout}}
    end
  end

  def handle_actor_down(s), do: {[], s}

  # Turn queued events into untagged responses. Pushes already sitting in the process
  # mailbox are taken in first, so a command sees the effects of its own writes.
  defp flush(%{state: :selected} = s, mode) do
    s = drain(s)
    {out, rest, s} = apply_events(s.pending, mode, s, [])
    {out, %{s | pending: rest}}
  end

  defp flush(s, _mode), do: {[], s}

  defp drain(%{folder: %{id: fid}} = s) do
    receive do
      {:mailstore, ^fid, event} -> drain(%{s | pending: s.pending ++ [event]})
    after
      0 -> s
    end
  end

  defp apply_events([], _mode, s, acc), do: {Enum.reverse(acc), [], s}

  defp apply_events([{:expunge, _, _} | _] = rest, :no_expunge, s, acc),
    do: {Enum.reverse(acc), rest, s}

  defp apply_events([event | rest], mode, s, acc) do
    {out, s} = apply_event(event, s)
    apply_events(rest, mode, s, [out | acc])
  end

  defp apply_event({:exists, uids, _view}, s) do
    before = UidMap.size(s.map)
    map = UidMap.append(s.map, uids)

    out =
      if UidMap.size(map) != before,
        do: ["* ", Integer.to_string(UidMap.size(map)), " EXISTS\r\n"],
        else: []

    {out, %{s | map: map}}
  end

  defp apply_event({:flags, changes, modseq}, s) do
    if MapSet.member?(s.own_modseqs, modseq) do
      {[], %{s | own_modseqs: MapSet.delete(s.own_modseqs, modseq)}}
    else
      out =
        for %{uid: uid, flags: flags} <- changes, seq = UidMap.seq(s.map, uid), seq != nil do
          [
            "* ",
            Integer.to_string(seq),
            " FETCH (UID ",
            Integer.to_string(uid),
            " FLAGS ",
            Format.flags(flags),
            ")\r\n"
          ]
        end

      {out, %{s | highest_modseq: max(s.highest_modseq, modseq)}}
    end
  end

  defp apply_event({:expunge, uids, modseq}, s) do
    {out, s} = remove(s, uids)
    {out, %{s | highest_modseq: max(s.highest_modseq, modseq || 0)}}
  end

  defp apply_event({:reload, _view}, s), do: resync(s)

  defp resync(s) do
    %{account: account, folder: folder} = s
    in_db = UidMap.from_ranges(Mailbox.uid_ranges(account.tenant_id, folder.id))
    known_max = UidMap.max_uid(s.map)
    # Rare path (actor restart, retention): walking the old map is acceptable here.
    gone = Enum.reject(UidMap.to_list(s.map), &UidMap.seq(in_db, &1))
    {expunges, s} = remove(s, gone)
    new = UidMap.uids_between(in_db, known_max + 1, UidMap.max_uid(in_db))
    {exists, s} = apply_event({:exists, new, nil}, s)

    changed = Mailbox.changed_since(account.tenant_id, folder.id, s.highest_modseq)

    flags =
      for c <- changed, seq = UidMap.seq(s.map, c.uid), seq != nil do
        [
          "* ",
          Integer.to_string(seq),
          " FETCH (UID ",
          Integer.to_string(c.uid),
          " FLAGS ",
          Format.flags(Flags.to_list(c.flags, c.keywords)),
          ")\r\n"
        ]
      end

    highest = Enum.reduce(changed, s.highest_modseq, &max(&1.modseq, &2))
    {[expunges, exists, flags], %{s | highest_modseq: highest, pending: []}}
  end

  defp remove(s, uids) do
    {seqs, map} = UidMap.remove(s.map, uids)
    {for(seq <- seqs, do: ["* ", Integer.to_string(seq), " EXPUNGE\r\n"]), %{s | map: map}}
  end

  defp own(s, changes) do
    modseqs = changes |> Enum.map(& &1.modseq) |> MapSet.new()

    %{
      s
      | own_modseqs: MapSet.union(s.own_modseqs, modseqs),
        highest_modseq: Enum.max([s.highest_modseq | MapSet.to_list(modseqs)])
    }
  end
end

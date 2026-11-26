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
  alias Mailstore.Core.{Flags, FolderName, NewMessage, UidMap}
  alias Mailstore.Imap.{Fetch, Format, Parser, Search, SequenceSet}

  @capabilities "IMAP4rev1 LITERAL+ IDLE UIDPLUS MOVE NAMESPACE ID ENABLE UNSELECT SPECIAL-USE CHILDREN"
  @system_flags ~w(\\Answered \\Flagged \\Deleted \\Seen \\Draft)

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
            idle_tag: nil,
            # Keywords announced in FLAGS, and UIDs that are \\Recent in this session.
            keywords: MapSet.new(),
            recent: MapSet.new()

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
    case folder_changes(s) do
      {:bye, out, s} -> {out, s}
      {:ok, s} -> run_command(command, s)
    end
  end

  # A rename or delete of the selected folder that is already waiting in the process
  # mailbox takes effect before the command, which would otherwise use the old name.
  defp folder_changes(%{state: :selected} = s) do
    s = drain(s)
    {now, later} = Enum.split_with(s.pending, &(&1 == :deleted or match?({:renamed, _}, &1)))
    s = %{s | pending: later}

    Enum.reduce(now, {:ok, s}, fn
      _event, {:bye, _, _} = done ->
        done

      event, {:ok, s} ->
        case apply_event(event, s) do
          {[], s} -> {:ok, s}
          {out, s} -> {:bye, out, s}
        end
    end)
  end

  defp folder_changes(s), do: {:ok, s}

  defp run_command(command, s) do
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
      if Mailbox.Unavailable.transient?(e) do
        Logger.warning("IMAP #{cmd}: storage unavailable: #{Exception.message(e)}")
        {[], {:no, "[UNAVAILABLE] Mail storage temporarily unavailable, try again"}, s}
      else
        Logger.error("IMAP #{cmd} failed: " <> Exception.format(:error, e, __STACKTRACE__))
        {[], {:no, "[SERVERBUG] Internal error"}, s}
      end
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

  defp command(cmd, [name], s) when cmd in ["SUBSCRIBE", "UNSUBSCRIBE"] do
    with {:ok, name} <- astring(name),
         :ok <- Mailbox.set_subscribed(s.account.id, name, cmd == "SUBSCRIBE") do
      {[], {:ok, "#{cmd} completed"}, s}
    else
      error -> folder_error(error, s)
    end
  end

  defp command("CREATE", [name], s) do
    with {:ok, name} <- astring(name),
         :ok <- Mailbox.create_folder(s.account.id, name) do
      {[], {:ok, "CREATE completed"}, s}
    else
      error -> folder_error(error, s)
    end
  end

  defp command("DELETE", [name], s) do
    with {:ok, name} <- astring(name) do
      # Deleting the selected folder unselects it first.
      s = if selected_folder?(s, name), do: unselect(s), else: s

      case Mailbox.delete_folder(s.account.id, name) do
        :ok -> {[], {:ok, "DELETE completed"}, s}
        error -> folder_error(error, s)
      end
    else
      error -> folder_error(error, s)
    end
  end

  defp command("RENAME", [old, new], s) do
    with {:ok, old} <- astring(old),
         {:ok, new} <- astring(new),
         :ok <- Mailbox.rename_folder(s.account.id, old, new) do
      {[], {:ok, "RENAME completed"}, s}
    else
      error -> folder_error(error, s)
    end
  end

  defp command(cmd, [name | _], s) when cmd in ["SELECT", "EXAMINE"] do
    case astring(name) do
      {:ok, name} -> select(s, name, cmd)
      :error -> {[], {:bad, "Invalid folder name"}, s}
    end
  end

  defp command(cmd, [ref, pattern], s) when cmd in ["LIST", "LSUB"] do
    with {:ok, ref} <- astring(ref), {:ok, pattern} <- astring(pattern) do
      out =
        if cmd == "LSUB" and pattern != "",
          do: lsub(s, ref <> pattern),
          else: list(s, cmd, ref <> pattern, pattern)

      {out, {:ok, "#{cmd} completed"}, s}
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

    case Mailbox.subscribe(s.account.id, name, read_only: read_only) do
      {:ok, actor, view} ->
        # The UIDs and first unseen come from the actor's memory, taken atomically with
        # the subscription: no database query, and every later change arrives as a push.
        map = view.uids

        unseen =
          with uid when is_integer(uid) <- view.first_unseen,
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
            highest_modseq: view.highest_modseq,
            keywords: MapSet.new(view.keywords),
            recent: MapSet.new(view.recent)
        }

        out = [
          flags_responses(s),
          ["* ", Integer.to_string(UidMap.size(map)), " EXISTS\r\n"],
          ["* ", Integer.to_string(MapSet.size(s.recent)), " RECENT\r\n"],
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
        own_modseqs: MapSet.new(),
        keywords: MapSet.new(),
        recent: MapSet.new()
    }
  end

  defp unselect(s), do: s

  # FLAGS and PERMANENTFLAGS: system flags plus the keywords used in the folder.
  defp flags_responses(s) do
    flags = @system_flags ++ Enum.sort(s.keywords)
    permanent = if s.folder.read_only, do: [], else: flags ++ ["\\*"]

    [
      ["* FLAGS ", Format.flags(flags), "\r\n"],
      ["* OK [PERMANENTFLAGS ", Format.flags(permanent), "] Flags permitted\r\n"]
    ]
  end

  # Flags as this session shows them: a keyword the client has not seen in FLAGS yet is
  # announced first (RFC 3501 7.2.6), and its own recent messages carry \\Recent.
  defp show_flags(s, rows) do
    new =
      for %{flags: flags} <- rows,
          flag <- flags,
          not String.starts_with?(flag, "\\"),
          not MapSet.member?(s.keywords, flag),
          uniq: true,
          do: flag

    {announce, s} =
      if new == [] do
        {[], s}
      else
        s = %{s | keywords: MapSet.union(s.keywords, MapSet.new(new))}
        {flags_responses(s), s}
      end

    rows =
      Enum.map(rows, fn row ->
        if MapSet.member?(s.recent, row.uid),
          do: %{row | flags: row.flags ++ ["\\Recent"]},
          else: row
      end)

    {announce, rows, s}
  end

  defp folder_error({:error, :invalid_name}, s), do: {[], {:no, "Invalid folder name"}, s}

  defp folder_error({:error, :no_such_folder}, s),
    do: {[], {:no, "[NONEXISTENT] No such folder"}, s}

  defp folder_error({:error, :already_exists}, s),
    do: {[], {:no, "[ALREADYEXISTS] Folder already exists"}, s}

  defp folder_error({:error, :inbox}, s), do: {[], {:no, "[CANNOT] Not allowed for INBOX"}, s}

  defp folder_error({:error, :has_children}, s),
    do: {[], {:no, "[HASCHILDREN] Folder has children, delete them first"}, s}

  defp folder_error({:error, :into_itself}, s),
    do: {[], {:no, "[CANNOT] Cannot move a folder into itself"}, s}

  defp folder_error(:error, s), do: {[], {:bad, "Invalid arguments"}, s}
  defp folder_error({:error, _}, s), do: {[], {:no, "[SERVERBUG] Folder operation failed"}, s}

  defp selected_folder?(%{state: :selected, folder: folder}, name),
    do: FolderName.normalize(name) == {:ok, folder.name}

  defp selected_folder?(_s, _name), do: false

  defp list(s, cmd, full_pattern, pattern) do
    if pattern == "" do
      [~s{* }, cmd, ~s{ (\\Noselect) "/" ""\r\n}]
    else
      regex = pattern_regex(full_pattern)
      folders = Mailbox.list_folders(s.account.tenant_id, s.account.id)
      names = Enum.map(folders, & &1.name)

      for f <- folders,
          Regex.match?(regex, f.name) or (f.name == "INBOX" and Regex.match?(regex, "inbox")) do
        children? = Enum.any?(names, &String.starts_with?(&1, f.name <> "/"))
        children = if children?, do: "\\HasChildren", else: "\\HasNoChildren"
        attrs = Enum.reject([children, f.special_use], &is_nil/1)
        ["* ", cmd, " (", Enum.join(attrs, " "), ~s{) "/" }, Format.string(f.name), "\r\n"]
      end
    end
  end

  # LSUB: subscribed folders. With "%", a folder that is not subscribed but has a
  # subscribed folder below it is listed as \Noselect (RFC 3501 6.3.9).
  defp lsub(s, pattern) do
    regex = pattern_regex(pattern)
    folders = Mailbox.list_folders(s.account.tenant_id, s.account.id)
    subscribed = for f <- folders, f.subscribed, do: f.name

    candidates =
      if String.contains?(pattern, "*"),
        do: subscribed,
        else: subscribed |> Enum.flat_map(&(FolderName.parents(&1) ++ [&1])) |> Enum.uniq()

    for name <- Enum.sort(candidates), Regex.match?(regex, name) do
      attrs = if name in subscribed, do: "", else: "\\Noselect"
      ["* LSUB (", attrs, ~s{) "/" }, Format.string(name), "\r\n"]
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
  defp status_item("RECENT", v), do: "RECENT #{v.recent}"
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

    {announce, msgs, s} = show_flags(s, msgs)

    out =
      for m <- msgs, seq = UidMap.seq(s.map, m.uid), seq != nil do
        items =
          if MapSet.member?(seen_now, m.uid) and :flags not in items,
            do: items ++ [:flags],
            else: items

        Fetch.render(seq, items, m, raws[m.uid])
      end

    {[announce, out], s}
  end

  defp store(%{folder: %{read_only: true}} = s, _, _, _, _, _),
    do: {[], {:no, "Folder is read-only"}, s}

  defp store(s, uids, mode, silent?, flags, uid?) do
    {:ok, changes} = Mailbox.store_flags(s.account.id, s.folder.name, uids, mode, flags)
    s = own(s, changes)

    {out, s} =
      if silent? do
        # Still announce new keywords: later FETCH FLAGS must not show unknown ones.
        {announce, _, s} = show_flags(s, Enum.map(changes, &%{uid: &1.uid, flags: &1.flags}))
        {announce, s}
      else
        rows = Mailbox.fetch_meta(s.account.tenant_id, s.folder.id, uids)
        {announce, rows, s} = show_flags(s, rows)

        out =
          for m <- rows, seq = UidMap.seq(s.map, m.uid), seq != nil do
            Fetch.render(seq, if(uid?, do: [:uid, :flags], else: [:flags]), m, nil)
          end

        {[announce, out], s}
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
    ctx = %{max_seq: UidMap.size(map), max_uid: UidMap.max_uid(map), recent: s.recent}
    candidates = Search.candidates(tree, map)

    hits =
      case Search.needs(tree) do
        :ids ->
          uids = if candidates == :all, do: UidMap.to_list(map), else: candidates
          msgs = for uid <- uids, do: %{uid: uid, seq: UidMap.seq(map, uid)}
          Search.run(tree, msgs, ctx)

        :flags ->
          if Search.only_unseen?(tree) do
            for uid <- unseen_uids(s),
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

  # From the actor's memory; the database only if the actor is unavailable.
  defp unseen_uids(%{account: account, folder: folder}) do
    case Mailbox.unseen(account.id, folder.name) do
      {:ok, unseen} -> UidMap.to_list(unseen)
      {:error, _} -> Mailbox.unseen_uids(account.tenant_id, folder.id)
    end
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
  # Rename and delete take effect at once: the next command must use the new name, and
  # a deleted folder ends the session (BYE may be sent at any time).
  def handle_push(
        {:mailstore, folder_id, event},
        %{state: :selected, folder: %{id: folder_id}} = s
      )
      when event == :deleted or elem(event, 0) == :renamed,
      do: apply_event(event, s)

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
    case Mailbox.subscribe(s.account.id, s.folder.name, read_only: s.folder.read_only) do
      {:ok, actor, view} -> resync(%{s | actor: actor}, view.uids)
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

  # New mail that is \\Recent in this session: EXISTS, then the new RECENT count.
  defp apply_event({:exists, uids, view, :recent}, s) do
    {out, s} = apply_event({:exists, uids, view}, s)
    new = Enum.filter(uids, &UidMap.seq(s.map, &1))
    s = %{s | recent: MapSet.union(s.recent, MapSet.new(new))}
    {[out, "* ", Integer.to_string(MapSet.size(s.recent)), " RECENT\r\n"], s}
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
      {announce, changes, s} = show_flags(s, changes)

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

      {[announce, out], %{s | highest_modseq: max(s.highest_modseq, modseq)}}
    end
  end

  defp apply_event({:expunge, uids, modseq}, s) do
    {out, s} = remove(s, uids)
    {out, %{s | highest_modseq: max(s.highest_modseq, modseq || 0)}}
  end

  defp apply_event({:reload, _view}, s), do: resync(s)
  defp apply_event({:renamed, view}, s), do: {[], put_in(s.folder.name, view.name)}

  defp apply_event(:deleted, s),
    do: {"* BYE Selected folder was deleted\r\n", %{unselect(s) | state: :logout}}

  # `current`: the folder's UIDs now, from a fresh subscription; nil reads them from the
  # database (after retention, when the actor has dropped its state).
  defp resync(s, current \\ nil) do
    %{account: account, folder: folder} = s
    in_db = current || UidMap.from_ranges(Mailbox.uid_ranges(account.tenant_id, folder.id))
    known_max = UidMap.max_uid(s.map)
    # Rare path (actor restart, retention): walking the old map is acceptable here.
    gone = Enum.reject(UidMap.to_list(s.map), &UidMap.seq(in_db, &1))
    {expunges, s} = remove(s, gone)
    new = UidMap.uids_between(in_db, known_max + 1, UidMap.max_uid(in_db))
    {exists, s} = apply_event({:exists, new, nil}, s)

    changed = Mailbox.changed_since(account.tenant_id, folder.id, s.highest_modseq)
    rows = Enum.map(changed, &%{uid: &1.uid, flags: Flags.to_list(&1.flags, &1.keywords)})
    {announce, rows, s} = show_flags(s, rows)

    flags =
      for c <- rows, seq = UidMap.seq(s.map, c.uid), seq != nil do
        [
          "* ",
          Integer.to_string(seq),
          " FETCH (UID ",
          Integer.to_string(c.uid),
          " FLAGS ",
          Format.flags(c.flags),
          ")\r\n"
        ]
      end

    highest = Enum.reduce(changed, s.highest_modseq, &max(&1.modseq, &2))
    {[expunges, exists, announce, flags], %{s | highest_modseq: highest, pending: []}}
  end

  defp remove(s, uids) do
    {seqs, map} = UidMap.remove(s.map, uids)
    recent = Enum.reduce(uids, s.recent, &MapSet.delete(&2, &1))

    {for(seq <- seqs, do: ["* ", Integer.to_string(seq), " EXPUNGE\r\n"]),
     %{s | map: map, recent: recent}}
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

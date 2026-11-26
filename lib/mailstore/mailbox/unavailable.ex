defmodule Mailstore.Mailbox.Unavailable do
  @moduledoc """
  The tenant's shard cannot take the request right now: the connection broke, the
  primary is restarting or has just become a replica (a failover). The write did not
  go through, unless the connection broke during the COMMIT itself (then it may have,
  as with any lost connection). The client may retry. IMAP answers
  `NO [UNAVAILABLE]` (RFC 5530).
  """
  defexception message: "mail storage temporarily unavailable"

  # Server errors that mean "try again later", not "this request is wrong".
  @transient_codes [
    :admin_shutdown,
    :crash_shutdown,
    :cannot_connect_now,
    :too_many_connections,
    :read_only_sql_transaction,
    :deadlock_detected,
    :serialization_failure,
    :lock_not_available,
    :query_canceled
  ]

  def transient_codes, do: @transient_codes

  @doc "Whether a database exception is transient (connection lost, failover, overload)."
  def transient?(%DBConnection.ConnectionError{}), do: true
  def transient?(%Postgrex.Error{postgres: %{code: code}}), do: code in @transient_codes
  def transient?(%__MODULE__{}), do: true
  def transient?(_), do: false
end

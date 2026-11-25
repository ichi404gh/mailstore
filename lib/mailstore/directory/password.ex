defmodule Mailstore.Directory.Password do
  @moduledoc """
  Password hashes in Dovecot-style `{SCHEME}` notation.

  Only PBKDF2-SHA256 for now. Importing accounts from an existing mail server needs its
  scheme (SHA512-CRYPT / BLF-CRYPT) added here.

  Polling clients log in often, so the IMAP layer caches successful verifications
  (`Mailstore.Directory.Cache`) instead of re-hashing each time.
  """
  @scheme "{PBKDF2-SHA256}"

  def hash(password, iterations \\ nil) when is_binary(password) do
    iterations = iterations || Application.get_env(:mailstore, :pbkdf2_iterations, 100_000)
    salt = :crypto.strong_rand_bytes(16)
    dk = :crypto.pbkdf2_hmac(:sha256, password, salt, iterations, 32)
    "#{@scheme}#{iterations}$#{Base.encode64(salt)}$#{Base.encode64(dk)}"
  end

  def verify(@scheme <> rest, password) when is_binary(password) do
    with [iter, salt, dk] <- String.split(rest, "$"),
         {iterations, ""} <- Integer.parse(iter),
         {:ok, salt} <- Base.decode64(salt),
         {:ok, dk} <- Base.decode64(dk) do
      :crypto.hash_equals(
        dk,
        :crypto.pbkdf2_hmac(:sha256, password, salt, iterations, byte_size(dk))
      )
    else
      _ -> false
    end
  end

  def verify(_hash, _password), do: false
end

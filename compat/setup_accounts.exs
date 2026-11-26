# Accounts for the compatibility runs (compat/README.md). Idempotent, and works while
# the server is running (this copy listens on a random port):
#   mix run --no-start compat/setup_accounts.exs
Application.load(:mailstore)
imap = Application.get_env(:mailstore, :imap, [])
Application.put_env(:mailstore, :imap, Keyword.merge(imap, port: 0, proxy_port: nil))
{:ok, _} = Application.ensure_all_started(:mailstore)
alias Mailstore.Directory

domain =
  Directory.get_domain_by_name("compat.test") ||
    (fn ->
       {:ok, tenant} = Directory.create_tenant(%{name: "compat"})
       {:ok, domain} = Directory.create_domain(tenant, "compat.test")
       domain
     end).()

for user <- ~w(imaptest imaptest2 client migrated) do
  case Directory.create_account(domain, user, %{password: "swordfish"}) do
    {:ok, a} -> IO.puts("created #{a.address}")
    {:error, _} -> IO.puts("exists  #{user}@compat.test")
  end
end

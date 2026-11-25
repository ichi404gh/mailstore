defmodule Mailstore.Directory.Queries do
  @moduledoc """
  Composable read queries over directory schemas. They only build `Ecto.Query`
  values; `Mailstore.Directory` runs them.
  """
  import Ecto.Query
  alias Mailstore.Directory.Schemas.{Account, Forwarding}

  def active_account_by_address(address) do
    from a in Account, where: a.address == ^String.downcase(address) and a.status == "active"
  end

  def enabled_forwardings_of(account_id) do
    from f in Forwarding,
      where: f.source_account_id == ^account_id and f.enabled,
      order_by: f.id
  end
end

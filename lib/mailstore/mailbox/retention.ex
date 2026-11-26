defmodule Mailstore.Mailbox.Retention do
  @moduledoc """
  Retention policies: how long mail is kept, and when its bodies may move to the cold
  tier. Configured, not hard-coded:

      config :mailstore, :retention,
        policies: [
          standard: [id: 0, keep_days: 365, cold_after_days: 30],
          short: [id: 1, keep_days: 30]
        ],
        # message class (Mailstore.Core.MessageClass) -> policy; the rest use :default
        classes: [warmup: :short],
        default: :standard

  * A message gets the policy of its class when it is stored, and keeps it (copies
    and moves too): changing `classes` affects new mail only.
  * `messages` and `message_bodies` are partitioned by policy, then by week of
    `stored_at` (`messages_p<id>_<IYYY>w<IW>`), so a policy's old weeks are dropped
    whole while other policies' messages of the same week stay.
  * `keep_days`: a week is dropped once it ended more than `keep_days` ago, so mail
    stays at least `keep_days` and at most a week longer.
  * `cold_after_days` (optional): bodies of a closed week older than that go to the
    object store (the cold tier, see docs/architecture.md); `nil` keeps them in
    Postgres until they expire.
  * `id` is the partition key and the partition name: never give an id to another
    policy. A policy removed from the config keeps its partitions until removed by
    hand.

  Expiry itself is `Mailstore.Mailbox.apply_retention/1`, run on a schedule by
  `Mailbox.PartitionMaintainer`.
  """
  alias Mailstore.Core.MessageClass

  defmodule Policy do
    @moduledoc false
    defstruct [:name, :id, :keep_days, :cold_after_days]
  end

  @key {__MODULE__, :config}

  @doc "All policies, by id."
  def policies, do: config().policies

  @doc "Ids of all policies (the partitions that must exist)."
  def ids, do: Enum.map(policies(), & &1.id)

  @doc "Policy id for a message class (atom or its integer)."
  def policy_id(class) when is_atom(class), do: policy_id(MessageClass.to_int(class))
  def policy_id(class) when is_integer(class), do: elem(config().by_class, class)

  @doc "Policies whose bodies go to the cold tier."
  def cold_policies, do: Enum.filter(policies(), & &1.cold_after_days)

  @doc """
  Read and check the configuration (raises on a bad one) and cache it. Called at start;
  call again after changing the application environment.
  """
  def load! do
    config = build(Application.get_env(:mailstore, :retention, []))
    :persistent_term.put(@key, config)
    config
  end

  defp config do
    case :persistent_term.get(@key, nil) do
      nil -> load!()
      config -> config
    end
  end

  @doc false
  def build(env) do
    policies =
      env
      |> Keyword.get(:policies, standard: [id: 0, keep_days: 365])
      |> Enum.map(fn {name, opts} -> policy(name, Map.new(opts)) end)
      |> Enum.sort_by(& &1.id)

    ids = Enum.map(policies, & &1.id)
    ids == Enum.uniq(ids) or raise ArgumentError, "retention: policy ids repeat: #{inspect(ids)}"
    by_name = Map.new(policies, &{&1.name, &1})
    default = Keyword.get(env, :default, :standard)

    Map.has_key?(by_name, default) or
      raise ArgumentError, "retention: no default policy #{inspect(default)}"

    classes = Map.new(Keyword.get(env, :classes, []))

    for {class, name} <- classes do
      class in MessageClass.all() or
        raise ArgumentError, "retention: unknown class #{inspect(class)}"

      Map.has_key?(by_name, name) or
        raise ArgumentError, "retention: unknown policy #{inspect(name)}"
    end

    by_class =
      MessageClass.all()
      |> Enum.sort_by(&MessageClass.to_int/1)
      |> Enum.map(&by_name[Map.get(classes, &1, default)].id)
      |> List.to_tuple()

    # MessageClass integers are 0..n-1, so the tuple index is the class.
    true = tuple_size(by_class) == length(MessageClass.all())
    %{policies: policies, by_class: by_class}
  end

  defp policy(name, opts) do
    id = Map.fetch!(opts, :id)
    keep = Map.fetch!(opts, :keep_days)
    cold = Map.get(opts, :cold_after_days)

    (is_integer(id) and id in 0..32_767) or
      raise ArgumentError, "retention #{name}: id must be 0..32767"

    (is_integer(keep) and keep > 0) or raise ArgumentError, "retention #{name}: keep_days > 0"

    cold == nil or (is_integer(cold) and cold >= 0 and cold < keep) or
      raise ArgumentError, "retention #{name}: cold_after_days must be below keep_days"

    %Policy{name: name, id: id, keep_days: keep, cold_after_days: cold}
  end
end

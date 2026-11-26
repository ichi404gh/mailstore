defmodule Mailstore.ColdStore do
  @moduledoc """
  Object store of the cold tier: whole pack files go in, byte ranges come out
  (`Mailstore.Mailbox.Cold`). Any store with the S3 API (MinIO, AWS S3, SeaweedFS,
  Ceph...) through `ColdStore.S3`, or a local directory (`ColdStore.Local`, for
  development, tests and single-machine installs).

      config :mailstore, :cold_store,
        adapter: Mailstore.ColdStore.S3,
        endpoint: "http://localhost:8333", bucket: "mailstore-cold", region: "us-east-1",
        access_key_id: "...", secret_access_key: "...",
        prefix: "cold"

      config :mailstore, :cold_store, adapter: Mailstore.ColdStore.Local, root: "/var/lib/mailstore/cold"

  Without `:cold_store` the cold tier is off: bodies stay in Postgres until they expire.
  """

  @callback put(config :: map, key :: String.t(), data :: binary) :: :ok | {:error, term}
  @callback get_range(
              config :: map,
              key :: String.t(),
              offset :: non_neg_integer,
              length :: pos_integer
            ) ::
              {:ok, binary} | {:error, term}
  @callback size(config :: map, key :: String.t()) ::
              {:ok, non_neg_integer} | {:error, :not_found | term}
  @callback delete(config :: map, key :: String.t()) :: :ok | {:error, term}

  @doc "Whether a cold store is configured."
  def enabled?, do: config() != nil

  @doc "Store `data` under `key`. The store checks it arrived intact (Content-MD5 on S3)."
  def put(key, data), do: call(:put, [key, data])

  @doc "`length` bytes of `key` from `offset`."
  def get_range(key, offset, length), do: call(:get_range, [key, offset, length])

  @doc "Size of the stored object."
  def size(key), do: call(:size, [key])

  @doc "Remove `key`; a missing key is not an error."
  def delete(key), do: call(:delete, [key])

  defp call(fun, args) do
    case config() do
      nil -> {:error, :cold_store_not_configured}
      %{adapter: adapter} = config -> apply(adapter, fun, [config | args])
    end
  end

  defp config do
    case Application.get_env(:mailstore, :cold_store) do
      nil -> nil
      [] -> nil
      opts -> Map.new(opts)
    end
  end
end

defmodule Mailstore.ColdStore.Local do
  @moduledoc "Cold store in a local directory (`root:`): one file per key."
  @behaviour Mailstore.ColdStore

  @impl true
  def put(%{root: root}, key, data) do
    path = Path.join(root, key)
    tmp = path <> ".part"
    File.mkdir_p!(Path.dirname(path))

    with :ok <- File.write(tmp, data, [:sync]),
         do: File.rename(tmp, path)
  end

  @impl true
  def get_range(%{root: root}, key, offset, length) do
    with {:ok, file} <- File.open(Path.join(root, key), [:read, :binary]) do
      try do
        case :file.pread(file, offset, length) do
          {:ok, data} when byte_size(data) == length -> {:ok, data}
          {:ok, _short} -> {:error, :short_read}
          :eof -> {:error, :short_read}
          error -> error
        end
      after
        File.close(file)
      end
    end
  end

  @impl true
  def size(%{root: root}, key) do
    case File.stat(Path.join(root, key)) do
      {:ok, %{size: size}} -> {:ok, size}
      {:error, :enoent} -> {:error, :not_found}
      error -> error
    end
  end

  @impl true
  def delete(%{root: root}, key) do
    case File.rm(Path.join(root, key)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      error -> error
    end
  end
end

defmodule Mailstore.ColdStore.S3 do
  @moduledoc """
  Cold store over the S3 API with path-style URLs (`endpoint/bucket/key`), signed with
  SigV4. Works with AWS S3, MinIO, SeaweedFS and the like. Uploads carry Content-MD5,
  so the store rejects a pack damaged on the way.
  """
  @behaviour Mailstore.ColdStore

  @impl true
  def put(config, key, data) do
    md5 = Base.encode64(:crypto.hash(:md5, data))

    case request(config, :put, key, body: data, headers: [{"content-md5", md5}]) do
      {:ok, %{status: 200}} -> :ok
      other -> error(other)
    end
  end

  @impl true
  def get_range(config, key, offset, length) do
    range = "bytes=#{offset}-#{offset + length - 1}"

    case request(config, :get, key, headers: [{"range", range}]) do
      {:ok, %{status: 206, body: body}} when byte_size(body) == length ->
        {:ok, body}

      {:ok, %{status: status, body: body}} when status in [200, 206] ->
        {:error, {:short_read, byte_size(body)}}

      other ->
        error(other)
    end
  end

  @impl true
  def size(config, key) do
    case request(config, :head, key) do
      {:ok, %{status: 200} = resp} ->
        {:ok, resp |> Req.Response.get_header("content-length") |> hd() |> String.to_integer()}

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      other ->
        error(other)
    end
  end

  @impl true
  def delete(config, key) do
    case request(config, :delete, key) do
      {:ok, %{status: status}} when status in [200, 204, 404] -> :ok
      other -> error(other)
    end
  end

  defp request(config, method, key, opts \\ []) do
    url = "#{String.trim_trailing(config.endpoint, "/")}/#{config.bucket}/#{object(config, key)}"

    Req.request(
      [
        method: method,
        url: url,
        aws_sigv4: [
          access_key_id: config[:access_key_id] || "",
          secret_access_key: config[:secret_access_key] || "",
          service: :s3,
          region: config[:region] || "us-east-1"
        ],
        retry: :transient,
        max_retries: 3,
        decode_body: false,
        compressed: false,
        receive_timeout: config[:timeout] || 60_000
      ] ++ opts
    )
  end

  defp object(config, key) do
    case config[:prefix] do
      nil -> key
      "" -> key
      prefix -> prefix <> "/" <> key
    end
  end

  defp error({:ok, %{status: status, body: body}}),
    do: {:error, {:http, status, String.slice(to_string(body), 0, 300)}}

  defp error({:error, reason}), do: {:error, reason}
end

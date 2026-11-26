defmodule Mailstore.ColdStoreTest do
  @moduledoc """
  `ColdStore.S3` against a real S3-compatible store. Excluded by default; run with

      docker run -d --name mailstore-s3 -p 8333:8333 chrislusf/seaweedfs:4.53 server -s3 -dir=/data
      curl -X PUT localhost:8333/mailstore-test
      mix test --only s3
  """
  use ExUnit.Case, async: false
  alias Mailstore.ColdStore.S3

  @moduletag :s3

  setup do
    {:ok, _} = Application.ensure_all_started(:req)

    config = %{
      adapter: S3,
      endpoint: System.get_env("MAILSTORE_TEST_S3_ENDPOINT", "http://localhost:8333"),
      bucket: System.get_env("MAILSTORE_TEST_S3_BUCKET", "mailstore-test"),
      access_key_id: System.get_env("MAILSTORE_TEST_S3_ACCESS_KEY_ID", "test"),
      secret_access_key: System.get_env("MAILSTORE_TEST_S3_SECRET_ACCESS_KEY", "test"),
      prefix: "test-#{System.unique_integer([:positive])}"
    }

    %{config: config}
  end

  test "put, size, ranges, delete", %{config: c} do
    data = :crypto.strong_rand_bytes(3_000_000)
    assert :ok = S3.put(c, "w/a.pack", data)
    assert S3.size(c, "w/a.pack") == {:ok, 3_000_000}
    assert S3.get_range(c, "w/a.pack", 0, 10) == {:ok, binary_part(data, 0, 10)}
    assert S3.get_range(c, "w/a.pack", 2_999_990, 10) == {:ok, binary_part(data, 2_999_990, 10)}
    assert :ok = S3.delete(c, "w/a.pack")
    assert S3.size(c, "w/a.pack") == {:error, :not_found}
    assert :ok = S3.delete(c, "w/a.pack")
  end

  test "the store rejects a body that does not match its Content-MD5", %{config: c} do
    url = "#{c.endpoint}/#{c.bucket}/#{c.prefix}/bad.pack"

    resp =
      Req.put!(url,
        body: "what arrived",
        headers: [{"content-md5", Base.encode64(:crypto.hash(:md5, "what was sent"))}],
        aws_sigv4: [
          access_key_id: c.access_key_id,
          secret_access_key: c.secret_access_key,
          service: :s3
        ],
        retry: false
      )

    assert resp.status == 400
    assert S3.size(c, "bad.pack") == {:error, :not_found}
  end
end

# Tests against a real S3-compatible store run with `mix test --only s3`
# (MAILSTORE_TEST_S3_ENDPOINT, default http://localhost:8333; see test/mailstore/cold_store_test.exs).
ExUnit.start(exclude: [:s3])

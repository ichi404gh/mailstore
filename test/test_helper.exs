# Tests against a real S3-compatible store run with `mix test --only s3`
# (MAILSTORE_TEST_S3_ENDPOINT, default http://localhost:8333; see test/mailstore/cold_store_test.exs).
# Tests against a real Kafka: `bench/ingest/kafka.sh start`, then `mix test --only kafka`
# (MAILSTORE_TEST_KAFKA, default localhost:9092).
ExUnit.start(exclude: [:s3, :kafka])

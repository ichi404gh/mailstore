#!/bin/sh
# Poller matrix for the mailstore vs Stalwart comparison (bench/README.md, "Сравнение со Stalwart").
# Start exactly one server first, seeded with the same accounts:
#   mailstore: elixir --sname mailstore --cookie loadtest --erl "+Q 131072 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt
#   stalwart:  bench/stalwart/run.sh
# then:  bench/stalwart/compare.sh mailstore|stalwart [run ...]
# Output goes to bench/stalwart/.run/results/<server>-<run>.txt; both are measured with
# --server-pid (ps), so the numbers are comparable.
set -e
server=$1; shift
cd "$(dirname "$0")/../loadgen"
out=../stalwart/.run/results
mkdir -p "$out"

common="--sessions 5000 --users 5000 --rate 300 --duration 30 --user-format user%d@cmp.test
  --password kettle-orbit-Quince-47 --pg-container mailstore-pg"

case $server in
  mailstore)
    pid=$(pgrep -f "sname mailstore")
    target="--port 1143 --server-pid $pid"
    deliver=""
    run_loadgen() { elixir --sname "loadgen$$" --cookie loadtest -S mix loadgen.poller $target $common --server-node "mailstore@$(hostname -s)" "$@"; }
    ;;
  stalwart)
    pid=$(cat ../stalwart/.run/stalwart.pid)
    target="--port 2143 --server-pid $pid"
    deliver="--deliver-lmtp 127.0.0.1:2424"
    run_loadgen() { mix loadgen.poller $target $common $deliver "$@"; }
    ;;
  *) echo "usage: $0 mailstore|stalwart [run ...]"; exit 1 ;;
esac

runs=${*:-"cycle noop-1k noop-5k select-1k select-5k unseen-1k unseen-5k deliver-0 deliver-500"}

for r in $runs; do
  case $r in
    # Typical sequencer: every 5 s NOOP, new UIDs, fetch them, mark seen; 100 deliveries/s.
    cycle)       args="--interval 5 --cycle noop,search_new,fetch_new,store_seen --deliver-rate 100" ;;
    # Price of one command: the same command at 1 000 and 5 000 commands/s, no deliveries.
    noop-1k)     args="--interval 5 --cycle noop" ;;
    noop-5k)     args="--interval 1 --cycle noop" ;;
    select-1k)   args="--interval 5 --cycle select" ;;
    select-5k)   args="--interval 1 --cycle select" ;;
    unseen-1k)   args="--interval 5 --cycle search_unseen" ;;
    unseen-5k)   args="--interval 1 --cycle search_unseen" ;;
    # Price of a delivery: rare NOOPs, with 0 and 500 deliveries/s.
    deliver-0)   args="--interval 30 --cycle noop --deliver-rate 0" ;;
    deliver-500) args="--interval 30 --cycle noop --deliver-rate 500" ;;
    *) echo "unknown run $r"; exit 1 ;;
  esac
  echo "== $server $r"
  run_loadgen $args > "$out/$server-$r.txt" 2>&1 || true
  grep -E "ramp finished|failure|errors|measured|^     [a-z_]+ +[0-9]|server process|server:|postgres" "$out/$server-$r.txt" || tail -5 "$out/$server-$r.txt"
done

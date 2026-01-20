#!/usr/bin/env bash

set -euo pipefail

MAELSTROM=./maelstrom/maelstrom
BIN=./target/debug
LOG_DIR="$(pwd)/logs"

if [ -d logs ]; then
  echo "===> Cleaning old logs"
  rm -rf "$LOG_DIR"
fi
mkdir -p "$LOG_DIR"

echo "===> Building project"
cargo b

echo "===> Running Maelstrom tests in parallel"

$MAELSTROM test -w echo \
  --bin $BIN/echo \
  --node-count 1 \
  --time-limit 10 \
  > logs/echo.log 2>&1 &

$MAELSTROM test -w unique-ids \
  --bin $BIN/unique_ids \
  --time-limit 30 \
  --rate 1000 \
  --node-count 3 \
  --availability total \
  --nemesis partition \
  > logs/unique-ids.log 2>&1 &

$MAELSTROM test -w broadcast \
  --bin $BIN/broadcast \
  --node-count 1 \
  --time-limit 20 \
  --rate 10 \
  > logs/broadcast-single.log 2>&1 &

$MAELSTROM test -w broadcast \
  --bin $BIN/multibroadcast \
  --node-count 5 \
  --time-limit 20 \
  --rate 10 \
  > logs/broadcast-multi.log 2>&1 &

$MAELSTROM test -w g-counter \
  --bin $BIN/gocounter \
  --node-count 3 \
  --rate 100 \
  --time-limit 20 \
  --nemesis partition \
  > logs/g-counter.log 2>&1 &

$MAELSTROM test -w kafka \
  --bin $BIN/kafkalog \
  --node-count 1 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  > logs/kafka-single.log 2>&1 &

$MAELSTROM test -w kafka \
  --bin $BIN/multikafkalog \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  > logs/kafka-multi.log 2>&1 &

$MAELSTROM test -w txn-rw-register \
  --bin $BIN/singletxn \
  --node-count 1 \
  --time-limit 20 \
  --rate 1000 \
  --concurrency 2n \
  --consistency-models read-uncommitted \
  --availability total \
  > logs/singletxn.log 2>&1 &

$MAELSTROM test -w txn-rw-register \
  --bin $BIN/multitxn \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  --consistency-models read-committed \
  --availability total \
  --nemesis partition \
  > logs/multitxn.log 2>&1 &

wait

echo "===> All Maelstrom tests completed"
echo "Logs available in ./logs/"

echo
echo "================= TEST REPORT ================="

declare -A TESTS=(
  [echo]="echo.log"
  [unique-ids]="unique-ids.log"
  [broadcast-single]="broadcast-single.log"
  [broadcast-multi]="broadcast-multi.log"
  [g-counter]="g-counter.log"
  [kafka-single]="kafka-single.log"
  [kafka-multi]="kafka-multi.log"
  [singletxn]="singletxn.log"
  [multitxn]="multitxn.log"
)

FAILED=0

for test in "${!TESTS[@]}"; do
  log="$LOG_DIR/${TESTS[$test]}"

  if tail -n 1 "$log" | grep -q "Everything looks good!"; then
    printf "✅ %-20s SUCCESS\n" "$test"
  else
    printf "❌ %-20s FAILED\n" "$test"
    FAILED=1
  fi
done

echo "==============================================="

if [ "$FAILED" -eq 1 ]; then
  echo "Some tests FAILED ❌"
  exit 1
else
  echo "All tests PASSED ✅"
fi
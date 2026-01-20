# Rustorm
> *A distributed system is one in which the failure of 
> a computer you didn't even know existed can render
> your own computer unusable.*

— **Leslie Lamport**

https://github.com/user-attachments/assets/b16e39ae-5c8f-4894-b89a-d2d45c52f4ba

Rustorm is a Rust implementation of the [Maelstrom](https://fly.io/dist-sys/) distributed systems challenges.

## Run Maelstrom Tests
- Install [Maelstrom](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md).

- Run `cargo b`.

Echo challenge:
```shell
maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10
```
Unique ID Generation challenge:
```shell
maelstrom test test -w unique-ids --bin ./target/debug/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```
Single-Node Broadcast challenge:
```shell
maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 1 --time-limit 20 --rate 10
```
Multi-Node Fault Tolerant Efficient Broadcast challenge:
```shell
maelstrom test -w broadcast --bin ./target/debug/multibroadcast --node-count 5 --time-limit 20 --rate 10
```
Grow-Only Counter challenge:
```shell
maelstrom test -w g-counter --bin ./target/debug/gocounter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```
Single-Node Kafka-Style Log Challenge:
```shell
maelstrom test -w kafka --bin ./target/debug/kafkalog --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
```
Multi-Node Efficient Kafka-Style Log Challenge:
```shell
maelstrom test -w kafka --bin ./target/debug/multikafkalog --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```
Single-Node, Totally-Available Transactions:
```shell
maelstrom test -w txn-rw-register --bin ./target/debug/singletxn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
```
Totally-Available, Read Uncommitted/Committed Transactions
```shell
maelstrom test -w txn-rw-register --bin ./target/debug/multitxn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total --nemesis partition
```
You can run all tests at once using `run_all_maelstrom_tests.sh` bash script.

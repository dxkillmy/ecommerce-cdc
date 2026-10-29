# Failure tests

I wanted to check that the pipeline doesn't lose or duplicate data when something crashes. After each test I ran `dbt run` and `checks/reconcile.py`, and compared row counts between the OLTP database and the warehouse.

## Test 0: Kafka lost all its data

At first Kafka had no volume, so recreating the container wiped every topic and the connector config. I registered the Debezium connector again, it took a new snapshot of the OLTP tables, and the consumer loaded everything again.

Result: passed. Reconcile matched.

What I learned: anything that keeps state needs a volume. I added one for Kafka after this.

## Test 1: Killed the consumer in the middle of a batch

I reset the consumer group to the beginning (~80k events), started the consumer and ran `kill -9` after a few batches.

Expected: the last batch would be read again, but the warehouse should end up the same because the upsert only applies events with a newer LSN.

Result: passed. order_items was 71,997 in OLTP and 71,997 in staging.
Reconcile passed.

Something I didn't expect: the new consumer did nothing for about 45 seconds. Kafka was still waiting for the old consumer's session to time out.

## Test 2: Restarted Kafka while orders were coming in

I ran the simulator to write 500 new orders and ran
`docker compose restart kafka` while it was still writing.

Expected: Debezium can't send anything while Kafka is down, but Postgres keeps the changes in the WAL because of the replication slot. When Kafka comes back, Debezium should continue from where it stopped.

What happened: the simulator didn't notice anything because it only writes to Postgres. The consumer printed connection errors and then carried on by itself. (check: did the Debezium task stay RUNNING?)

Result: passed. orders 4,000 and order_items 82,941 on both sides.
Reconcile passed.

## Test 3: Restarted the warehouse database while the consumer was writing

I ran the simulator again (500 more orders) and ran
`docker compose restart warehouse-db` in the middle.

Expected: the consumer crashes because its database connection is gone, but nothing is lost. The batch it was writing gets rolled back by Postgres, and its offsets were never committed, so the batch is read again after arestart.

What happened: the consumer crashed with
`OperationalError: server closed the connection unexpectedly`, then
`InterfaceError: connection already closed` when it tried to roll back on the dead connection. I started it again by hand and it caught up.

Result: passed. orders 4,500 and order_items 92,827 in staging.
Reconcile passed.

Problem left: someone has to restart the consumer manually. It should recover by itself.

## Test 3 again: Consumer running in Docker with a restart policy

After test 3 I moved the consumer into Docker with `restart: unless-stopped`, so it should come back by itself after a crash.

My first try didn't prove anything. The consumer container had been recreated after I restarted the database, so it never saw the broken connection and RestartCount was 0.

Second try: I watched `docker compose logs -f consumer`, ran the simulator up to 6,500 orders, and restarted the warehouse database while the consumer was processing small batches. The logs showed the `OperationalError`, then the consumer started again on its own. RestartCount was 1.

Result: passed. orders 6,500 in staging. Reconcile passed.

## Test 4: Debezium was down while orders came in

I stopped the Connect container, wrote 500 new orders, then started it again.

While Debezium was down the replication slot was inactive and Postgres was keeping 3,319 kB of WAL for it. After I started Connect again the slot became active and Debezium sent the missed changes.

Result: passed. orders 5,000 in staging. Reconcile passed.

Something to watch: the slot keeps WAL until Debezium reads it. If the connector stays down for a long time, the source database can run out of disk. In a real setup I would alert on the retained WAL size.
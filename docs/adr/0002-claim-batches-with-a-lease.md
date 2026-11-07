# 2. Claim batches with a lease, skipping locked rows

Date: 2026-11-07

## Status

Accepted

## Context

The relay runs inside the service, so a service with three instances has three relays reading the same outbox table. If two of them read the same pending rows, both publish them. Consumers deduplicate, but sending every message several times on purpose wastes the broker and makes the duplicates the normal case instead of the crash case.

The options I looked at:

- One relay only, picked by a leader election or a distributed lock. Simple to reason about, but throughput is capped at one instance and failover waits for the lock to expire.
- Hold the rows locked (`FOR UPDATE` / `UPDLOCK`) for the whole batch, inside one transaction that stays open while the messages go to the broker. No extra columns, but a slow broker keeps a database transaction and its locks open for seconds.
- Claim the rows in one short statement that sets a `claimed_until` column, commit straight away, then send. The claim is a lease: if the relay dies, the rows come back when it runs out.

## Decision

- `IOutboxStore.ClaimAsync` claims up to a batch of the oldest pending rows by setting `claimed_until`, and only takes rows that are unclaimed or whose claim has run out. `ClaimDuration` (30 seconds by default) sets the length.
- The claim is one statement: an `UPDATE` over a `SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL, and an `UPDATE` through a `TOP` CTE with `UPDLOCK, READPAST, ROWLOCK` on SQL Server. Two relays claiming at the same moment skip each other's rows instead of waiting or taking the same ones.
- The dispatcher stops sending when the claim runs out halfway through a batch, because another relay may have claimed the rest.
- Both stores sit in the core package and take a `DbDataSource`, so it doesn't reference Npgsql or SqlClient.
- Times come from the app's `TimeProvider`, not the database clock, so tests can move time.

## Consequences

- Any number of instances can run the relay, and a relay that dies only delays its batch by one claim duration.
- A failed send also waits for the claim to run out before the message is tried again. Retries with backoff come with the dead-letter work and will replace that.
- Relays compare claims with their own clocks, so the claim has to be far longer than the clock skew between instances. Seconds of skew against a 30 second claim is fine.
- A batch that takes longer than the claim to send can still overlap with another relay for the message in flight. That stays inside at-least-once.
- Two relays can each claim messages with the same ordering key. Ordering per key across instances is a separate task.
- The table needs `claimed_until` and `dispatched_at` columns. The EF Core mapping adds them as shadow properties.

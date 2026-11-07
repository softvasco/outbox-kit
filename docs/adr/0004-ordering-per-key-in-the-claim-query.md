# 4. Ordering per key lives in the claim query

Date: 2026-11-07

## Status

Accepted

## Context

A message can carry an ordering key, usually the id of the aggregate that raised it (`order-42`). A consumer of `OrderPlaced`, `OrderPaid` and `OrderShipped` for the same order expects them in that order. With several relays claiming batches, nothing stopped relay A from taking `OrderPlaced` while relay B took `OrderPaid` and sent it first. A failed send made it worse: the failed message waited for its retry while the next one for its key went out.

Messages for different keys have no order between them, and they should keep going out in parallel.

## Decision

- A pending message is claimable only when no older pending message has the same key. Older means `created_at`, then `id`, the same order the claim already uses. Pending means not dispatched and not dead-lettered, whether claimed or not.
- It is one `NOT EXISTS` in the claim query on both engines. The check doesn't look at claims, so it can't race: while the oldest message for a key is out with relay A, or waiting for its retry, the next one is not a candidate for any relay. It becomes one when the older row is marked dispatched.
- A batch therefore holds at most one message per key. Different keys still spread across relays through `SKIP LOCKED` and `READPAST` as before ([ADR 2](0002-claim-batches-with-a-lease.md)).
- Messages without a key are not ordered. `NULL = NULL` is not true in SQL, so they never wait on anything.
- A dead letter stops holding back its key, as [ADR 3](0003-retries-and-dead-letters-in-the-outbox-table.md) already decided.

## Consequences

- One busy key moves one message per poll. A full batch is followed by the next one straight away, so this is a round trip per message for that key, not a polling interval. If one key ever needs more, the claim could take the oldest run of a key in one go, at the price of a harder query.
- The order is the order of `created_at`. Two transactions writing to the same key at the same time can commit in the other order; the outbox can't see that, and keeping writes to one aggregate in sequence is the application's job anyway.
- The `NOT EXISTS` wants an index on `(ordering_key, created_at)` over pending rows. The schema scripts and indexes are a separate piece of work.

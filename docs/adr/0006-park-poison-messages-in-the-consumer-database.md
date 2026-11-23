# 6. Park poison messages in the consumer's database

Date: 2026-11-23

## Status

Accepted

## Context

A message the consumer can never handle (a bug, a reference to data that doesn't exist yet) comes back from the broker until its delivery count runs out. Then the broker dead-letters it: Service Bus has a dead-letter subqueue, RabbitMQ a dead-letter exchange if one is set up, Kafka nothing at all. Each one is read and replayed with different tools, and replaying from there sends the message back to the topic, so every subscription gets it again.

The consumer already has a database and an inbox table. Whoever fixes the bug wants to see what failed and why, then run the fixed handler on exactly those messages.

## Decision

- A `parked_messages` table keyed by message id and consumer, like the inbox, with the type, the payload, the error and when it was parked.
- The consumer decides when to give up, usually from the broker's delivery count, parks the message in its own transaction and acks it. The library doesn't count attempts: the broker already does, and a count kept in the database would roll back with the failed handler.
- Parking the same message twice keeps the first row. A redelivery that fails the same way changes nothing.
- `TakeAsync` deletes the row in the caller's transaction and returns it, so a replay or a discard is one transaction. Two people replaying the same message at once get it once: the second delete waits on the row and finds nothing.
- `ReplayAsync` takes the row and runs the handler through `HandleOnceAsync` in the same transaction. The message isn't sent back through the broker, so other consumers never see it again.
- Parking doesn't record the message in the inbox. If it did, the replay would find it there and skip the handler.

## Consequences

- No broker dead-letter tooling is needed, and it works the same on every transport.
- A message parked and then redelivered (a crash between the commit and the ack) runs the handler once more. If it succeeds that time, the parked row is stale; a later replay finds the id in the inbox, deletes the row and doesn't run the handler.
- The payload is stored as it came off the broker. A handler fixed by changing the message contract has to read the old shape too.
- Nothing deletes parked rows on its own. They are meant to be looked at.

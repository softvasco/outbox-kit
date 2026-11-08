# Delivery guarantees

The outbox delivers every message at least once. It never loses one that was committed, and it can send one more than once. This page lists when a duplicate happens and what a consumer has to do about it.

## Why a committed message can't be lost

The message row is written in the same transaction as the business change ([ADR 1](adr/0001-transactional-outbox.md)). Once that commits, the row stays pending until one of two things happens:

- a send succeeds and the dispatcher sets `dispatched_at`;
- sends fail `MaxAttempts` times and the dispatcher sets `dead_lettered_at` ([ADR 3](adr/0003-retries-and-dead-letters-in-the-outbox-table.md)).

A relay that dies holding a batch only holds it until `claimed_until` passes, then any relay can claim it again ([ADR 2](adr/0002-claim-batches-with-a-lease.md)). A database or broker outage just means nothing moves until it's back.

A dead-lettered message is not delivered, but it's not gone either: the row keeps the payload and `last_error`, so it can be inspected and sent again by hand. Admin endpoints for that are tracked in [#21](https://github.com/softvasco/outbox-kit/issues/21).

## When a message arrives twice

The dispatcher marks a message dispatched after the transport says the send worked. Anything that breaks between the broker accepting the message and that update means the message goes out again later:

| What happens | Why it's sent again |
| --- | --- |
| The process dies right after the send | The row is still pending and its claim runs out |
| The `UPDATE` that marks it dispatched fails (database down, failover) | Same as above |
| The send times out but the broker did get the message | The dispatcher counts a failure and retries after the backoff |
| The host shuts down while a send is in flight | A cancelled send isn't counted as a failure; the message waits for its claim to run out |
| A send takes longer than `ClaimDuration` | Another relay can claim the message and send it while the first send is still going |

The first four are normal and cheap: the duplicate comes some seconds later, and messages for the same ordering key still arrive in order, because the next one for that key isn't claimed until the earlier one is marked dispatched ([ADR 4](adr/0004-ordering-per-key-in-the-claim-query.md)).

The last one is the one to avoid. The dispatcher won't start a send once its claim has run out, but it can't stop one that's already in flight. If the second relay sends the message and then the next one for the same key, the slow first send can land after both, so the consumer sees A, B, A. Keep `ClaimDuration` well above the slowest send you expect (the default is 30 seconds), and give the transport a timeout shorter than that.

## What a consumer has to do

Treat every message as possibly seen before.

- Use the outbox id as the message id. `OutboxMessage.Id` is a UUID v7 fixed when the message is saved, and it's the same on every resend. A transport should put it in the broker's message id (`MessageId` on Service Bus and RabbitMQ, a header on Kafka), so the consumer and the broker's own duplicate detection both see it.
- Record handled ids in the same transaction as the work. Check whether the id is already there before handling the message, and write it with your own changes in one transaction. If the handler crashes, neither the work nor the id is saved and the redelivery does the work. If it commits, the redelivery finds the id and is acked without doing anything. That's the inbox, which this library will ship too ([#16](https://github.com/softvasco/outbox-kit/issues/16)).
- Don't rely on the broker alone. Broker duplicate detection (Service Bus has it, Kafka's idempotent producer covers producer retries) only looks back over a window and only at the broker. It doesn't help with a resend that comes after the window, and it doesn't cover the consumer crashing after its own commit and before the ack.
- Make the handler safe to repeat where it's cheap. Setting a status is naturally idempotent; adding to a balance isn't. Where a handler can be written as "set to" rather than "add", a duplicate does no harm even without the inbox.

## What isn't promised

- Exactly-once delivery to the broker. No outbox can promise that without the broker taking part in the transaction.
- Order across different ordering keys, or for messages saved without a key.
- Order when a send outlives its claim (see above).
- Any delivery when the business data and the outbox are in different databases. The guarantee comes from the shared transaction.

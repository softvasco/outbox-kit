# 7. The inbox is optional when the handler is idempotent on its own

Date: 2026-11-25

## Status

Accepted

## Context

The relay delivers at least once, so every consumer has to cope with a message it already handled. There are two ways to do that.

The inbox records the message id per consumer in the handler's transaction and skips the handler when the id is already there. It works for any handler, but it costs an insert (and on SQL Server a locked read) per message, a table that grows until retention cleans it, and it only protects writes that go to the same database as the inbox.

Some handlers are idempotent without it. An upsert keyed by the business id, an update guarded by a version or a status (`WHERE status = 'Pending'`), or setting a value rather than adding to it all give the same result the second time. Others are not: adding to a balance, appending a row, sending an email.

## Decision

- The inbox is a separate piece the consumer opts into per handler. Nothing in the dispatcher or the transports assumes it.
- Use the inbox when the handler increments, appends or has side effects that can't be checked afterwards, and its writes live in one database.
- Skip it when the write is naturally idempotent, and say so next to the handler, because the next change to it may not be.
- When the handler calls something outside the database (an HTTP API, an email provider), the inbox doesn't help with that call. Pass the message id on as the idempotency key, or record the call first and send it from an outbox of the consumer's own.
- A guard like `WHERE version = @expected` counts as natural idempotency only if a stale message is meant to be dropped. If it should fail loudly, the inbox plus an exception is clearer.

## Consequences

- Naturally idempotent handlers pay nothing for deduplication and keep working after the inbox rows are gone.
- With the inbox, a duplicate older than `RetainFor` (14 days by default) runs the handler again. The window has to be longer than anything the broker or a replay can bring back.
- Which handlers rely on the inbox is a decision in the code, not a setting, so a handler that stops being idempotent needs its review to catch it. The duplicate-delivery tests are the safety net: each handler should have one that delivers the same message twice.

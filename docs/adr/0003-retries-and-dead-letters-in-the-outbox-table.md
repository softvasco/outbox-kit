# 3. Retries and dead letters live in the outbox table

Date: 2026-11-07

## Status

Accepted

## Context

Until now a failed send left the message claimed, and it was tried again when the claim ran out. That gave every failure the same 30 second wait, no matter whether the broker blipped once or the message could never be sent. A message that always fails also stays at the head of its batch forever, and with ordering per key coming next it would block every later message for its key.

## Decision

- Each failed send is counted in an `attempts` column. The message carries the count, so the dispatcher can work out the wait without asking the store again.
- The wait after a failure is `FirstDelay` doubled per earlier failure, capped at `MaxDelay` (1 second and 5 minutes by default). The retry time goes into `claimed_until`. The claim query already skips rows whose `claimed_until` is in the future, so it needs no new condition and no new index for retries.
- After `MaxAttempts` failed sends (10 by default) the message is dead-lettered: `dead_lettered_at` is set and the claim query never takes it again. The row stays in the table with `last_error`, so someone can read why and decide what to do.
- `last_error` holds the exception message only. Stack traces belong in the logs, which already get the exception.
- A send cancelled because the host is stopping doesn't count as a failure.
- The policy is `OutboxRetryPolicy` on the dispatcher options, validated at start like the rest of them.

## Consequences

- No jitter. A batch stops at its first failure, so a broker outage marks one message per batch, and their retry times already differ by when each batch ran. If many relays start failing at the same moment this may need jitter after all.
- There is no API to put a dead letter back yet. Clearing `dead_lettered_at` (and `attempts` if you want the full count again) is a plain `UPDATE`.
- Three more columns on the table: `attempts` (not null, default 0), `dead_lettered_at` and `last_error`. The EF Core mapping adds them, `attempts` as a property of the message and the other two as shadow properties.
- A dead letter takes its message out of the order for its key. Dropping it is better than letting it block every later message for that key, but the consumer can then see a later message without the earlier one.

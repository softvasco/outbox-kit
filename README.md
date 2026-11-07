# OutboxKit

[![ci](https://github.com/softvasco/outbox-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/softvasco/outbox-kit/actions/workflows/ci.yml)
[![license](https://img.shields.io/github/license/softvasco/outbox-kit)](LICENSE)

Transactional outbox and inbox for .NET. A message is saved in the same database transaction as the data that caused it, a relay delivers it at least once, and the consumer side makes sure it is handled once.

Early days: messages can be saved with the caller's transaction (ADO.NET or EF Core), and a hosted dispatcher claims them from PostgreSQL or SQL Server and sends them through a transport you plug in. Retries, ordering per key and the inbox come next, so nothing ships ready to publish yet.

## Why

A handler that saves an order and then publishes `OrderPlaced` to a broker does two writes that can't share a transaction. If the process dies between them, or the broker is down, the order exists and nobody hears about it. Publishing first is worse: the event goes out for an order that was rolled back.

The outbox fixes the producer side by writing the message to a table in the same transaction as the order. A relay reads that table and publishes. That gives at-least-once delivery, so the consumer side needs an inbox that remembers which messages it already handled.

Most .NET teams get this from a full messaging framework. I want the outbox and inbox on their own, small enough to read in an evening, working the same way on SQL Server and PostgreSQL, with tests that kill the relay halfway through a batch.

## How it fits

```mermaid
flowchart LR
    subgraph producer[Producer service]
        handler[handler] -->|one transaction| db[(business tables<br/>+ outbox)]
        relay[relay] -->|lease a batch| db
    end
    relay -->|publish, at least once| broker[[broker]]
    subgraph consumer[Consumer service]
        broker --> inbox{seen this<br/>message id?}
        inbox -->|no| work[handle and record<br/>in one transaction]
        inbox -->|yes| skip[ack and skip]
        work --> cdb[(consumer tables<br/>+ inbox)]
    end
```

## Saving a message

With ADO.NET, pass the transaction you already have. The row is written on its connection, so it commits or rolls back with your own writes:

```csharp
await using var transaction = await connection.BeginTransactionAsync(ct);
// ... insert the order with the same transaction
await outbox.EnqueueAsync(new OrderPlaced(order.Id), transaction, orderingKey: $"order-{order.Id}", ct);
await transaction.CommitAsync(ct);
```

With EF Core, map the table once and enqueue on the context. The message is only tracked; the next `SaveChanges` writes it together with your entities:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddOutboxMessages();

db.Orders.Add(order);
dbContextOutbox.Enqueue(db, new OrderPlaced(order.Id));
await db.SaveChangesAsync(ct);
```

Both write the same table and columns (`outbox_messages` by default), so one service can use either.

## Sending messages

The dispatcher runs as a hosted service. It claims a batch of pending messages from an `IOutboxStore`, hands each one to your `IMessageTransport` in order, and marks it dispatched after the send succeeds:

```csharp
builder.Services.AddSingleton<IOutboxStore>(new PostgreSqlOutboxStore(NpgsqlDataSource.Create(connectionString), new OutboxTable()));
// or new SqlServerOutboxStore(SqlClientFactory.Instance.CreateDataSource(connectionString), new OutboxTable())
builder.Services.AddSingleton<IMessageTransport, ServiceBusTransport>();
builder.Services.AddOutboxDispatcher(o =>
{
    o.PollingInterval = TimeSpan.FromSeconds(1);
    o.BatchSize = 100;
    o.ClaimDuration = TimeSpan.FromSeconds(30);
});
```

Every instance of the service can run the dispatcher. A claim sets `claimed_until` on the batch in one statement, using `FOR UPDATE SKIP LOCKED` on PostgreSQL and `READPAST` on SQL Server, so relays polling at the same time get different messages instead of waiting on each other. If an instance dies, its batch is claimed again once the claim runs out. [ADR 2](docs/adr/0002-claim-batches-with-a-lease.md) has the reasoning.

A full batch is followed by the next one straight away; it only waits for the polling interval once the outbox is drained. A failed send stops the batch, so later messages for the same key don't overtake it, and the service logs the error; the unsent messages go out again once their claim runs out. A crash between the send and the update sends that message again, which is why the consumer side needs an inbox.

## Plan

- Core model and an `IOutbox` that enqueues inside the caller's transaction (ADO.NET and EF Core).
- A relay that leases batches with `SKIP LOCKED` on PostgreSQL and `READPAST` on SQL Server, so several instances can run at once.
- Ordering per key, retries with backoff, and a dead-letter state.
- Transports behind one port, starting with an in-memory one for tests.
- An inbox keyed by message id and consumer name.
- The same contract tests run against both databases with Testcontainers.

## Building

```bash
dotnet build
dotnet test
```

Needs the .NET 10 SDK. The store tests start PostgreSQL and SQL Server in Docker through Testcontainers.

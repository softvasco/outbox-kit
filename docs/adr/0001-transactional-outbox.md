# 1. Transactional outbox instead of dual writes or distributed transactions

Date: 2026-11-04

## Status

Accepted

## Context

A service that changes its database and tells other services about it does two writes: one to its tables, one to a broker. They can't share a local transaction. If the process dies between the two, or the broker is down, one write happens and the other doesn't.

The options I looked at:

- Write to the database, then publish. A crash after the commit loses the message, and nobody notices.
- Publish, then write. A failed commit leaves an event out there for a change that never happened.
- A distributed transaction (two-phase commit across the database and the broker). Most brokers people use with .NET today (RabbitMQ, Kafka, Azure Service Bus) don't take part in one, it isn't available across platforms in .NET, and it holds locks while it waits on the network.
- Change data capture on the business tables. It works, but it ties the message format to the table layout and needs extra infrastructure (Debezium, SQL Server CDC) that a small team may not run.

## Decision

- The message is a row in an outbox table, written in the same local transaction as the business change. If the transaction rolls back, the message was never there.
- The library writes that row with whatever the caller already uses: `AdoNetOutbox` takes the caller's `DbTransaction`, and `DbContextOutbox` adds the message to the DbContext so the next `SaveChanges` writes it with the entities. Both use the same table and columns.
- A separate relay reads the table and publishes. It may publish a message more than once (after a crash between publish and marking it sent), so delivery is at least once and consumers deduplicate with an inbox.
- The core package has no EF Core dependency; EF Core support is its own package.

## Consequences

- No message is lost and none is published for a rolled-back change, as long as the business data and the outbox live in the same database.
- Consumers must handle duplicates. That is the price of at-least-once, and the inbox is part of this library for that reason.
- Messages arrive later than a direct publish, by up to one relay polling interval.
- The outbox table grows and needs cleanup of sent rows.
- It doesn't help when the business data is spread over several databases; each one needs its own outbox.

# 5. One package per transport

Date: 2026-11-20

## Status

Accepted

## Context

The dispatcher sends through `IMessageTransport`, a single method that takes an `OutboxMessage`. Three transports exist now (in-memory, RabbitMQ, Azure Service Bus) and Kafka is planned. Each real one needs its broker's client: `RabbitMQ.Client`, `Azure.Messaging.ServiceBus` with Azure.Core behind it, and later `Confluent.Kafka` with its native librdkafka binaries.

A service talks to one broker, sometimes two. If every client came with the core package, a RabbitMQ shop would restore the Azure SDK and the Kafka native libraries for nothing, and every major release of any client would force a new core release.

The options I looked at:

- Everything in `OutboxKit`. One package to install, but every app carries every client, and the core's version moves with all of them.
- Core plus one package per transport. More packages to version and publish, but an app pulls only the client it uses.
- No transports at all, only the port. Small, but then every user writes the same publisher confirms and MessageId handling, which is the part that's easy to get wrong.

## Decision

- `OutboxKit` holds the model, the stores, the dispatcher and the port. Its only dependencies are `Microsoft.Extensions.Hosting.Abstractions` and `Microsoft.Extensions.Options`. The stores take a `DbDataSource`, so the database driver is the app's choice too.
- Each broker gets its own project and package, `OutboxKit.<Broker>`, referencing the core and its own client and nothing else. EF Core support follows the same rule (`OutboxKit.EntityFrameworkCore`).
- `InMemoryTransport` stays in the core: it needs no client, and tests of any app want it.
- Every transport runs the same `TransportContract` tests against its real broker in Testcontainers, so what the dispatcher relies on (id, type, payload and ordering key arrive, send order holds, a failed send throws) is checked the same way for all of them.

## Consequences

- An app installs `OutboxKit` and the one transport it needs. A breaking release of `RabbitMQ.Client` only means a new `OutboxKit.RabbitMQ`.
- There are more packages to keep in step. release-please has to version them separately, and a change to the port touches every transport in the same pull request.
- A transport can't add anything to the core. Whatever it needs from the message has to be on `OutboxMessage` already, which keeps the port honest.
- Someone can write a transport for a broker this repo doesn't ship (an HTTP endpoint, a cloud queue) without forking anything.

# Contributing

Thanks for looking. Issues and pull requests are welcome.

## Before you start

For anything bigger than a typo, open an issue first so we can agree on the approach. Design choices that are hard to reverse get an ADR in `docs/adr/`.

## Working on the code

```bash
dotnet build
dotnet test
```

- .NET 10 SDK. Warnings are errors, so the build has to be clean.
- Time comes from `TimeProvider`, never `DateTime.Now`. Money, if any, is always `decimal`.
- Domain rules have unit tests. Anything that touches a database or a network gets an integration test.
- Keep pull requests focused on one change.

## Adding a transport

A transport goes in its own project (`OutboxKit.<Broker>`) so nobody pulls in a client library they don't use. Its tests derive from `TransportContract` in `tests/OutboxKit.Tests/Transports/Contract` and run against the real broker in Testcontainers. The contract checks what the dispatcher relies on: the outbox id, type, payload and ordering key reach the consumer, messages arrive in send order, a cancelled send throws and sends nothing, and a send to a destination that doesn't exist throws, so the dispatcher retries it instead of marking it sent. Anything specific to the broker (routing, sessions, confirms) goes in its own test class next to it.

## Commits and pull requests

Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, with a scope when it helps (`feat(domain): ...`). CI must be green before merge.

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

## Commits and pull requests

Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, with a scope when it helps (`feat(domain): ...`). CI must be green before merge.

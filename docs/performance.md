# Dispatch throughput

How long one relay takes to send 2,000 messages, by batch size. The transport discards every message, so this is the database side only: claiming a batch, then marking each message dispatched after its send. A real broker adds its own time per message.

Each run drains the whole table, so every iteration refills it and there is one warmup run and ten measured runs (`RunStrategy.Monitoring`). Both databases run in Testcontainers on the same laptop: PostgreSQL 18 and SQL Server 2022 with `READ_COMMITTED_SNAPSHOT` on.

```bash
dotnet run -c Release --project benchmarks/OutboxKit.Benchmarks -- --filter '*'
```

Measured on 2026-11-13, AMD Ryzen 7 7840HS, Windows 11, .NET 10.0.15, Docker Desktop. Times are the median of the ten runs.

| Batch size | PostgreSQL | msg/s | SQL Server | msg/s |
|-----------:|-----------:|------:|-----------:|------:|
| 1          | 3.42 s     | 584   | 11.09 s    | 180   |
| 10         | 1.49 s     | 1,344 | 3.85 s     | 519   |
| 100        | 3.29 s     | 608   | 3.62 s     | 552   |
| 500        | 1.74 s     | 1,151 | 2.91 s     | 686   |

## What the numbers say

On SQL Server, going from 1 to 10 is three times faster and the gains stop there. A batch only saves the claim query. Every message still costs its own `UPDATE` to mark it dispatched, and that round trip is most of what is left at 10 and above. Marking a batch in one statement is the obvious next step; it changes what a crash in the middle of a batch resends, so it needs its own tests first.

PostgreSQL doesn't get faster in a straight line: 10 and 500 are the fastest and 100 is as slow as 1. I haven't found why yet. The first two runs at batch size 10 took 21 s and 15 s, and the other eight about 1.5 s, in this run and in an earlier one, which looks like the claim query's plan changing after the first few executions. Until that is explained, take the PostgreSQL column as one measurement on one machine, not a rule for picking a batch size.

The raw BenchmarkDotNet reports are not committed; run the command above to get them for your own machine.

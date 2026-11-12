using System.Data.Common;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using OutboxKit.Stores;

namespace OutboxKit.Benchmarks;

// one run drains a full table, so each iteration needs fresh rows and the warmup counts as a real run
[SimpleJob(RunStrategy.Monitoring, warmupCount: 1, iterationCount: 10)]
public abstract class DispatchBenchmarks
{
    private const int Messages = 2_000;
    private static readonly TimeSpan ClaimDuration = TimeSpan.FromSeconds(30);

    private readonly OutboxTable _table = new();
    private DbDataSource _dataSource = null!;
    private OutboxDispatcher _dispatcher = null!;
    private AdoNetOutbox _outbox = null!;

    [Params(1, 10, 100, 500)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task StartDatabase()
    {
        _dataSource = await StartAsync();
        await using var create = _dataSource.CreateCommand(CreateTableSql(_table.Name));
        await create.ExecuteNonQueryAsync();

        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<PaymentAccepted>("payment.accepted")]);
        _outbox = new AdoNetOutbox(new OutboxMessageFactory(registry, new JsonMessageSerializer(), TimeProvider.System), _table);
        _dispatcher = new OutboxDispatcher(CreateStore(_dataSource, _table), new DiscardingTransport(), TimeProvider.System);
    }

    [GlobalCleanup]
    public Task StopDatabase() => StopAsync();

    [IterationSetup]
    public void FillOutbox() => FillOutboxAsync().GetAwaiter().GetResult();

    // the time for the whole table, so messages per second is Messages divided by the mean
    [Benchmark]
    public async Task<int> DispatchAll()
    {
        var total = 0;
        int sent;
        while ((sent = await _dispatcher.DispatchBatchAsync(BatchSize, ClaimDuration, CancellationToken.None)) > 0)
        {
            total += sent;
        }

        return total == Messages ? total : throw new InvalidOperationException($"Dispatched {total} of {Messages} messages.");
    }

    protected abstract Task<DbDataSource> StartAsync();

    protected abstract Task StopAsync();

    protected abstract IOutboxStore CreateStore(DbDataSource dataSource, OutboxTable table);

    protected abstract string CreateTableSql(string table);

    private async Task FillOutboxAsync()
    {
        await using var truncate = _dataSource.CreateCommand($"TRUNCATE TABLE {_table.Name}");
        await truncate.ExecuteNonQueryAsync();

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        for (var i = 0; i < Messages; i++)
        {
            await _outbox.EnqueueAsync(new PaymentAccepted($"pay-{i}", 125.40m, "EUR"), transaction);
        }

        await transaction.CommitAsync();
    }
}

public sealed record PaymentAccepted(string PaymentId, decimal Amount, string Currency);

// measures the database side only; a real broker adds its own time per message
internal sealed class DiscardingTransport : IMessageTransport
{
    public Task SendAsync(OutboxMessage message, CancellationToken cancellationToken) => Task.CompletedTask;
}

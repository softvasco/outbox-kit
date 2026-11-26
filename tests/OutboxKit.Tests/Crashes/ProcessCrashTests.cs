using System.Diagnostics;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using OutboxKit.CrashHost;
using OutboxKit.Stores;
using OutboxKit.Tests.Stores;

namespace OutboxKit.Tests.Crashes;

// a real process killed with FailFast: no finally blocks, no disposal, no flush, just like a pod being killed
public sealed class ProcessCrashTests(PostgreSqlServer server) : IClassFixture<PostgreSqlServer>, IAsyncLifetime
{
    private readonly string _suffix = Guid.NewGuid().ToString("N");

    private string OutboxTable => $"outbox_{_suffix}";

    private string OrdersTable => $"orders_{_suffix}";

    private string BrokerTable => $"broker_{_suffix}";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => await Execute($"""
        CREATE TABLE {OutboxTable} (
            id uuid PRIMARY KEY, type varchar(256) NOT NULL, payload text NOT NULL, created_at timestamptz NOT NULL,
            ordering_key varchar(256), claimed_until timestamptz, dispatched_at timestamptz,
            attempts int NOT NULL DEFAULT 0, dead_lettered_at timestamptz, last_error text);
        CREATE TABLE {OrdersTable} (id int PRIMARY KEY);
        CREATE TABLE {BrokerTable} (message_id uuid NOT NULL, type varchar(256) NOT NULL);
        """);

    public async ValueTask DisposeAsync() =>
        await Execute($"DROP TABLE {OutboxTable}; DROP TABLE {OrdersTable}; DROP TABLE {BrokerTable};");

    [Fact]
    public async Task A_process_killed_before_the_commit_leaves_neither_the_order_nor_the_message()
    {
        var crash = await RunCrashHost("before-commit");

        Assert.Contains("before the commit", crash, StringComparison.Ordinal);
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {OrdersTable}"));
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {OutboxTable}"));
    }

    [Fact]
    public async Task A_process_killed_right_after_the_commit_leaves_the_message_for_the_next_relay()
    {
        var crash = await RunCrashHost("after-commit");

        Assert.Contains("killed after commit", crash, StringComparison.Ordinal);
        Assert.Equal(1L, await Scalar($"SELECT count(*) FROM {OrdersTable}"));
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {BrokerTable}"));

        await DispatchFromANewProcess();

        Assert.Equal(["orders.placed"], await BrokerTypes());
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {OutboxTable} WHERE dispatched_at IS NULL"));
    }

    [Fact]
    public async Task A_process_killed_holding_a_claim_delays_the_message_but_sends_it_once()
    {
        await RunCrashHost("after-commit");

        var crash = await RunCrashHost("after-claim");

        Assert.Contains("after the claim", crash, StringComparison.Ordinal);
        Assert.Equal(1L, await Scalar($"SELECT count(*) FROM {OutboxTable} WHERE claimed_until IS NOT NULL"));

        await DispatchFromANewProcess();

        Assert.Equal(["orders.placed"], await BrokerTypes());
    }

    [Fact]
    public async Task A_process_killed_between_the_send_and_the_mark_sends_again_with_the_same_id()
    {
        await RunCrashHost("after-commit");

        var crash = await RunCrashHost("after-send");

        Assert.Contains("killed after the send", crash, StringComparison.Ordinal);
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {OutboxTable} WHERE dispatched_at IS NOT NULL"));

        await DispatchFromANewProcess();

        var ids = await BrokerIds();
        Assert.Equal(2, ids.Count);
        Assert.Single(ids.Distinct());
        Assert.Equal(ids[0], (Guid)(await Scalar($"SELECT id FROM {OutboxTable} WHERE dispatched_at IS NOT NULL"))!);
    }

    // the dead process's claim runs out; a fake clock past it stands in for waiting
    private async Task DispatchFromANewProcess()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.AddMinutes(5));
        var dispatcher = new OutboxDispatcher(
            new PostgreSqlOutboxStore(server.DataSource, new OutboxTable(OutboxTable)),
            new BrokerTableTransport(server.DataSource, BrokerTable),
            clock);

        Assert.Equal(1, await dispatcher.DispatchBatchAsync(10, TimeSpan.FromSeconds(30), Token));
    }

    private async Task<string> RunCrashHost(string mode)
    {
        var host = Path.Combine(AppContext.BaseDirectory, "OutboxKit.CrashHost.dll");
        var start = new ProcessStartInfo("dotnet")
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        foreach (var argument in new[] { host, mode, server.ConnectionString, OutboxTable, OrdersTable, BrokerTable })
        {
            start.ArgumentList.Add(argument);
        }

        // FailFast would otherwise write a crash dump for every run
        start.Environment["DOTNET_DbgEnableMiniDump"] = "0";

        using var process = Process.Start(start)!;
        var stderr = process.StandardError.ReadToEndAsync(Token);
        await process.StandardOutput.ReadToEndAsync(Token);
        await process.WaitForExitAsync(Token);

        // 0 or the host's own codes would mean it exited normally, so the crash never happened
        Assert.NotEqual(0, process.ExitCode);
        Assert.NotEqual(3, process.ExitCode);
        return await stderr;
    }

    private async Task<List<string>> BrokerTypes()
    {
        await using var command = server.DataSource.CreateCommand($"SELECT type FROM {BrokerTable}");
        await using var reader = await command.ExecuteReaderAsync(Token);
        var types = new List<string>();
        while (await reader.ReadAsync(Token))
        {
            types.Add(reader.GetString(0));
        }

        return types;
    }

    private async Task<List<Guid>> BrokerIds()
    {
        await using var command = server.DataSource.CreateCommand($"SELECT message_id FROM {BrokerTable}");
        await using var reader = await command.ExecuteReaderAsync(Token);
        var ids = new List<Guid>();
        while (await reader.ReadAsync(Token))
        {
            ids.Add(reader.GetGuid(0));
        }

        return ids;
    }

    private async Task<object?> Scalar(string sql)
    {
        await using var command = server.DataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(Token);
    }

    private async Task Execute(string sql)
    {
        await using var command = server.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(Token);
    }
}

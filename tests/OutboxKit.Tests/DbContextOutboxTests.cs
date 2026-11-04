using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Time.Testing;
using OutboxKit.EntityFrameworkCore;

namespace OutboxKit.Tests;

public sealed class DbContextOutboxTests : IAsyncLifetime
{
    private sealed record OrderPlaced(int OrderId);

    private sealed class Order
    {
        public int Id { get; set; }
    }

    private sealed class ShopContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Order>().Property(o => o.Id).ValueGeneratedNever();
            modelBuilder.AddOutboxMessages();
        }
    }

    private sealed class ContextWithoutOutbox(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();
    }

    // one open connection keeps the in-memory database alive across contexts
    private readonly SqliteConnection _connection = new("Data Source=:memory:");
    private readonly DbContextOutbox _outbox;

    public DbContextOutboxTests()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);
        _outbox = new DbContextOutbox(new OutboxMessageFactory(registry, new JsonMessageSerializer(), new FakeTimeProvider()));
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await _connection.OpenAsync(Token);
        await using var context = Shop();
        await context.Database.EnsureCreatedAsync(Token);
    }

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();

    [Fact]
    public async Task The_message_is_saved_by_the_same_save_changes_as_the_entity()
    {
        await using (var context = Shop())
        {
            context.Orders.Add(new Order { Id = 42 });
            _outbox.Enqueue(context, new OrderPlaced(42), "order-42");
            await context.SaveChangesAsync(Token);
        }

        Assert.Equal(["orders.placed|{\"orderId\":42}|order-42"], await OutboxRows());
    }

    [Fact]
    public async Task Nothing_is_written_before_save_changes()
    {
        await using (var context = Shop())
        {
            _outbox.Enqueue(context, new OrderPlaced(42));
        }

        Assert.Empty(await OutboxRows());
    }

    [Fact]
    public async Task A_failed_save_changes_keeps_the_message_out_too()
    {
        await using (var context = Shop())
        {
            context.Orders.Add(new Order { Id = 42 });
            await context.SaveChangesAsync(Token);
        }

        await using (var context = Shop())
        {
            context.Orders.Add(new Order { Id = 42 });
            _outbox.Enqueue(context, new OrderPlaced(42));
            await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(Token));
        }

        Assert.Empty(await OutboxRows());
    }

    [Fact]
    public async Task Nothing_is_saved_when_an_explicit_transaction_rolls_back()
    {
        await using (var context = Shop())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Token);
            context.Orders.Add(new Order { Id = 42 });
            _outbox.Enqueue(context, new OrderPlaced(42));
            await context.SaveChangesAsync(Token);
            await transaction.RollbackAsync(Token);
        }

        Assert.Empty(await OutboxRows());
    }

    [Fact]
    public async Task A_context_that_does_not_map_the_outbox_is_refused_with_the_fix()
    {
        await using var context = new ContextWithoutOutbox(Options());

        var error = Assert.Throws<InvalidOperationException>(() => _outbox.Enqueue(context, new OrderPlaced(42)));
        Assert.Contains("AddOutboxMessages", error.Message, StringComparison.Ordinal);
    }

    private ShopContext Shop() => new(Options());

    private DbContextOptions Options() => new DbContextOptionsBuilder().UseSqlite(_connection).Options;

    private async Task<List<string>> OutboxRows()
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT type, payload, ordering_key FROM outbox_messages";
        await using var reader = await command.ExecuteReaderAsync(Token);

        var rows = new List<string>();
        while (await reader.ReadAsync(Token))
        {
            var key = reader.IsDBNull(2) ? "<null>" : reader.GetString(2);
            rows.Add($"{reader.GetString(0)}|{reader.GetString(1)}|{key}");
        }

        return rows;
    }
}

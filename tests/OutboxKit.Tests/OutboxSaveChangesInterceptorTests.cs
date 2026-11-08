using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Time.Testing;
using OutboxKit.EntityFrameworkCore;

namespace OutboxKit.Tests;

public sealed class OutboxSaveChangesInterceptorTests : IAsyncLifetime
{
    private sealed record OrderPlaced(int OrderId);

    private sealed record OrderCancelled(int OrderId);

    private sealed class Order : IHasOutboxMessages
    {
        private readonly List<PendingOutboxMessage> _raised = [];

        public int Id { get; private set; }

        public static Order Place(int id)
        {
            var order = new Order { Id = id };
            order._raised.Add(new PendingOutboxMessage(new OrderPlaced(id), $"order-{id}"));
            return order;
        }

        public void Cancel() => _raised.Add(new PendingOutboxMessage(new OrderCancelled(Id), $"order-{Id}"));

        public IReadOnlyList<PendingOutboxMessage> TakeOutboxMessages()
        {
            var taken = _raised.ToList();
            _raised.Clear();
            return taken;
        }
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

    // one open connection keeps the in-memory database alive across contexts
    private readonly SqliteConnection _connection = new("Data Source=:memory:");
    private readonly OutboxSaveChangesInterceptor _interceptor;

    public OutboxSaveChangesInterceptorTests()
    {
        var registry = new MessageTypeRegistry(
        [
            MessageTypeRegistration.For<OrderPlaced>("orders.placed"),
            MessageTypeRegistration.For<OrderCancelled>("orders.cancelled"),
        ]);
        var factory = new OutboxMessageFactory(registry, new JsonMessageSerializer(), new FakeTimeProvider());
        _interceptor = new OutboxSaveChangesInterceptor(new DbContextOutbox(factory));
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
    public async Task Messages_an_entity_raised_are_saved_with_it()
    {
        await using (var context = Shop())
        {
            context.Orders.Add(Order.Place(42));
            await context.SaveChangesAsync(Token);
        }

        Assert.Equal(["orders.placed|{\"orderId\":42}|order-42"], await OutboxRows());
        Assert.Equal(1, await Count("orders"));
    }

    [Fact]
    public async Task The_sync_save_changes_picks_them_up_too()
    {
        await using (var context = Shop())
        {
            context.Orders.Add(Order.Place(42));
            context.SaveChanges();
        }

        Assert.Equal(["orders.placed|{\"orderId\":42}|order-42"], await OutboxRows());
    }

    [Fact]
    public async Task A_message_is_written_once_even_when_the_context_saves_again()
    {
        await using (var context = Shop())
        {
            var order = Order.Place(42);
            context.Orders.Add(order);
            await context.SaveChangesAsync(Token);

            order.Cancel();
            await context.SaveChangesAsync(Token);
        }

        Assert.Equal(
            ["orders.placed|{\"orderId\":42}|order-42", "orders.cancelled|{\"orderId\":42}|order-42"],
            await OutboxRows());
    }

    [Fact]
    public async Task An_entity_loaded_and_changed_later_raises_into_the_outbox()
    {
        await using (var context = Shop())
        {
            context.Orders.Add(Order.Place(42));
            await context.SaveChangesAsync(Token);
        }

        await using (var context = Shop())
        {
            var order = await context.Orders.SingleAsync(o => o.Id == 42, Token);
            order.Cancel();
            await context.SaveChangesAsync(Token);
        }

        Assert.Equal(2, await Count("outbox_messages"));
    }

    [Fact]
    public async Task A_failed_save_changes_writes_no_message()
    {
        await using (var context = Shop())
        {
            context.Orders.Add(Order.Place(42));
            await context.SaveChangesAsync(Token);
        }

        await using (var context = Shop())
        {
            context.Orders.Add(Order.Place(42));
            await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(Token));
        }

        Assert.Equal(1, await Count("outbox_messages"));
    }

    private ShopContext Shop() =>
        new(new DbContextOptionsBuilder().UseSqlite(_connection).AddInterceptors(_interceptor).Options);

    private async Task<long> Count(string table)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)(await command.ExecuteScalarAsync(Token))!;
    }

    private async Task<List<string>> OutboxRows()
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT type, payload, ordering_key FROM outbox_messages ORDER BY rowid";
        await using var reader = await command.ExecuteReaderAsync(Token);

        var rows = new List<string>();
        while (await reader.ReadAsync(Token))
        {
            rows.Add($"{reader.GetString(0)}|{reader.GetString(1)}|{reader.GetString(2)}");
        }

        return rows;
    }
}

using Microsoft.EntityFrameworkCore;

namespace OutboxKit.EntityFrameworkCore;

/// <summary>
/// Adds outbox messages to a DbContext's change tracker. Nothing is written until SaveChanges, which saves them
/// in the same transaction as the entities it saves.
/// </summary>
public sealed class DbContextOutbox
{
    private readonly OutboxMessageFactory _factory;

    public DbContextOutbox(OutboxMessageFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _factory = factory;
    }

    public OutboxMessage Enqueue(DbContext context, object message, string? orderingKey = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(message);

        if (context.Model.FindEntityType(typeof(OutboxMessage)) is null)
        {
            throw new InvalidOperationException(
                $"{context.GetType().Name} doesn't map outbox messages. Call modelBuilder.AddOutboxMessages() in OnModelCreating.");
        }

        var outboxMessage = _factory.Create(message, orderingKey);
        context.Set<OutboxMessage>().Add(outboxMessage);
        return outboxMessage;
    }
}

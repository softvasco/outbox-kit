using Microsoft.EntityFrameworkCore;

namespace OutboxKit.EntityFrameworkCore;

public static class OutboxModelBuilderExtensions
{
    /// <summary>Maps <see cref="OutboxMessage"/> to the outbox table, so rows are saved by the same SaveChanges as your entities.</summary>
    public static ModelBuilder AddOutboxMessages(this ModelBuilder modelBuilder, OutboxTable? table = null)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var (schema, name) = Split((table ?? new OutboxTable()).Name);

        // same table and columns as AdoNetOutbox, so a service can write through either one
        modelBuilder.Entity<OutboxMessage>(message =>
        {
            message.ToTable(name, schema);
            message.HasKey(m => m.Id);
            message.Property(m => m.Id).HasColumnName("id").ValueGeneratedNever();
            message.Property(m => m.Type).HasColumnName("type").HasMaxLength(OutboxMessage.MaxTypeLength).IsRequired();
            message.Property(m => m.Payload).HasColumnName("payload").IsRequired();
            message.Property(m => m.CreatedAt).HasColumnName("created_at");
            message.Property(m => m.OrderingKey).HasColumnName("ordering_key").HasMaxLength(OutboxMessage.MaxOrderingKeyLength);

            // only the stores read these, but a migration from this model has to create them
            message.Property<DateTimeOffset?>("ClaimedUntil").HasColumnName("claimed_until");
            message.Property<DateTimeOffset?>("DispatchedAt").HasColumnName("dispatched_at");
        });

        return modelBuilder;
    }

    private static (string? Schema, string Name) Split(string qualifiedName)
    {
        var dot = qualifiedName.IndexOf('.', StringComparison.Ordinal);
        return dot < 0 ? (null, qualifiedName) : (qualifiedName[..dot], qualifiedName[(dot + 1)..]);
    }
}

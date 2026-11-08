namespace OutboxKit.EntityFrameworkCore;

/// <summary>A message an entity has raised and that the next SaveChanges should put in the outbox.</summary>
public sealed record PendingOutboxMessage(object Message, string? OrderingKey = null);

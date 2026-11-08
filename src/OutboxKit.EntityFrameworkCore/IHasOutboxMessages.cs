namespace OutboxKit.EntityFrameworkCore;

/// <summary>An entity that raises messages, picked up by <see cref="OutboxSaveChangesInterceptor"/>.</summary>
public interface IHasOutboxMessages
{
    /// <summary>Returns the messages raised since the last call and forgets them.</summary>
    IReadOnlyList<PendingOutboxMessage> TakeOutboxMessages();
}

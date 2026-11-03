using System.Data.Common;

namespace OutboxKit;

/// <summary>Saves messages in the caller's transaction, so they exist only if the business change commits.</summary>
public interface IOutbox
{
    Task<OutboxMessage> EnqueueAsync(
        object message, DbTransaction transaction, string? orderingKey = null, CancellationToken cancellationToken = default);
}

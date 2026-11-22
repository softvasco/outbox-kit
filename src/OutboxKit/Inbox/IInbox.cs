using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Remembers which messages a consumer has handled, in the same transaction as the handler's own writes.</summary>
public interface IInbox
{
    /// <summary>
    /// Records the message for this consumer and returns true, or returns false when it was already recorded.
    /// A delivery of the same message running at the same time waits until this transaction ends.
    /// </summary>
    Task<bool> TryRecordAsync(string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default);
}

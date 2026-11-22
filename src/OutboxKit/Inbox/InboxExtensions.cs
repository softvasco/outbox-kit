using System.Data.Common;

namespace OutboxKit.Inbox;

public static class InboxExtensions
{
    /// <summary>
    /// Runs the handler unless this consumer already handled the message, and returns whether it ran.
    /// The handler must write with the same transaction, and the caller commits it and then acks the message.
    /// </summary>
    public static async Task<bool> HandleOnceAsync(
        this IInbox inbox,
        string messageId,
        string consumer,
        DbTransaction transaction,
        Func<DbTransaction, CancellationToken, Task> handle,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inbox);
        ArgumentNullException.ThrowIfNull(handle);

        // record before handling: a second delivery then blocks on the row instead of running the handler alongside
        if (!await inbox.TryRecordAsync(messageId, consumer, transaction, cancellationToken))
        {
            return false;
        }

        await handle(transaction, cancellationToken);
        return true;
    }
}

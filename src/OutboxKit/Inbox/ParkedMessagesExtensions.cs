using System.Data.Common;

namespace OutboxKit.Inbox;

public static class ParkedMessagesExtensions
{
    /// <summary>Takes a parked message and handles it through the inbox; true if it ran.</summary>
    public static async Task<bool> ReplayAsync(
        this IParkedMessages parked,
        IInbox inbox,
        string messageId,
        string consumer,
        DbTransaction transaction,
        Func<ParkedMessage, DbTransaction, CancellationToken, Task> handle,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(parked);
        ArgumentNullException.ThrowIfNull(inbox);
        ArgumentNullException.ThrowIfNull(handle);

        var message = await parked.TakeAsync(messageId, consumer, transaction, cancellationToken);
        if (message is null)
        {
            return false;
        }

        // a redelivery may have been handled after the fix; then the inbox skips it
        return await inbox.HandleOnceAsync(
            message.MessageId,
            message.Consumer,
            transaction,
            (tx, ct) => handle(message, tx, ct),
            cancellationToken);
    }
}

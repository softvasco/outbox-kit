using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Messages a consumer gave up on, kept aside so later ones get through.</summary>
public interface IParkedMessages
{
    /// <summary>Parks the message in the caller's transaction; a second park is ignored.</summary>
    Task ParkAsync(ParkedMessage message, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>The consumer's parked messages, oldest first.</summary>
    Task<IReadOnlyList<ParkedMessage>> ListAsync(string consumer, int limit, CancellationToken cancellationToken = default);

    /// <summary>Deletes the message in the caller's transaction and returns it, if parked.</summary>
    Task<ParkedMessage?> TakeAsync(string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default);
}

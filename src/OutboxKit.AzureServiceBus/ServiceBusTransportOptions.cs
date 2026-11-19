namespace OutboxKit.AzureServiceBus;

public sealed class ServiceBusTransportOptions
{
    /// <summary>The queue or topic every message is sent to.</summary>
    public string EntityName { get; set; } = string.Empty;

    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// Sends the ordering key as the SessionId, for a session-enabled queue or subscription whose consumers need the
    /// messages of one key in order. Leave it off for entities without sessions, which refuse a SessionId.
    /// </summary>
    public bool OrderingKeyAsSessionId { get; set; }

    /// <summary>
    /// How long a send may take, retries included, before it counts as failed. Keep it well under the dispatcher's
    /// ClaimDuration, or a slow send can land after another relay has sent the message again.
    /// </summary>
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(10);

    internal bool IsValid =>
        !string.IsNullOrWhiteSpace(EntityName) && !string.IsNullOrWhiteSpace(ContentType) && SendTimeout > TimeSpan.Zero;

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(EntityName);
        ArgumentException.ThrowIfNullOrWhiteSpace(ContentType);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(SendTimeout, TimeSpan.Zero, nameof(SendTimeout));
    }
}

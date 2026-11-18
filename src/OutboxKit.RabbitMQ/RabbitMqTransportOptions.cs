namespace OutboxKit.RabbitMQ;

public sealed class RabbitMqTransportOptions
{
    /// <summary>The exchange every message is published to. The empty string is the default exchange, which routes by queue name.</summary>
    public string Exchange { get; set; } = string.Empty;

    /// <summary>Picks the routing key for a message. Defaults to the registered message type, so a topic exchange can bind on it.</summary>
    public Func<OutboxMessage, string> RoutingKey { get; set; } = message => message.Type;

    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// How long to wait for the broker to confirm a publish before the send counts as failed. Keep it well under the
    /// dispatcher's ClaimDuration, or a slow send can land after another relay has sent the message again.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(10);

    internal bool IsValid =>
        Exchange is not null && RoutingKey is not null && !string.IsNullOrWhiteSpace(ContentType) && ConfirmTimeout > TimeSpan.Zero;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Exchange);
        ArgumentNullException.ThrowIfNull(RoutingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(ContentType);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ConfirmTimeout, TimeSpan.Zero, nameof(ConfirmTimeout));
    }
}

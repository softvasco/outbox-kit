using System.Text;
using RabbitMQ.Client;

namespace OutboxKit.RabbitMQ;

/// <summary>
/// Publishes outbox messages to RabbitMQ with publisher confirms. A send only returns once the broker has the message,
/// and a nack, an unroutable message or a missing confirm throws, so the dispatcher retries it.
/// </summary>
public sealed class RabbitMqTransport : IMessageTransport, IAsyncDisposable, IDisposable
{
    public const string OrderingKeyHeader = "outbox-ordering-key";

    private static readonly CreateChannelOptions ConfirmedChannel = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);

    private readonly IConnection _connection;
    private readonly RabbitMqTransportOptions _options;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IChannel? _channel;

    // the connection stays the caller's, so one connection can serve the transport and the consumers
    public RabbitMqTransport(IConnection connection, RabbitMqTransportOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        options ??= new RabbitMqTransportOptions();
        options.Validate();
        _connection = connection;
        _options = options;
    }

    public async Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.ConfirmTimeout);

        // one publish at a time keeps the channel's confirm tracking simple and the send order intact
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var channel = await OpenChannelAsync(timeout.Token).WaitAsync(timeout.Token);

            // mandatory, so a message no queue is bound for comes back as a failed send instead of vanishing
            await channel.BasicPublishAsync(
                _options.Exchange,
                _options.RoutingKey(message),
                mandatory: true,
                Properties(message),
                Encoding.UTF8.GetBytes(message.Payload),
                timeout.Token).AsTask().WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // a frozen broker can keep the client waiting past the token, and a confirm may still come
            // for this publish later, so the channel can't be trusted for the next message
            DropChannel();
            throw new TimeoutException($"RabbitMQ didn't confirm message {message.Id} within {_options.ConfirmTimeout}.");
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        _gate.Dispose();
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _gate.Dispose();
    }

    private void DropChannel()
    {
        var channel = _channel;
        _channel = null;
        if (channel is null)
        {
            return;
        }

        // closing waits for the broker as well, so it happens in the background
        _ = Task.Run(async () =>
        {
            try
            {
                await channel.AbortAsync();
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                // the broker is the reason the channel is dropped, so it failing to close is expected
            }
            finally
            {
                channel.Dispose();
            }
        });
    }

    private async Task<IChannel> OpenChannelAsync(CancellationToken cancellationToken)
    {
        // a channel closes on a broker error (an exchange that doesn't exist, say), and the next send needs a fresh one
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        _channel = await _connection.CreateChannelAsync(ConfirmedChannel, cancellationToken);
        return _channel;
    }

    private BasicProperties Properties(OutboxMessage message)
    {
        var properties = new BasicProperties
        {
            MessageId = message.Id.ToString(),
            Type = message.Type,
            ContentType = _options.ContentType,
            Persistent = true,
            Timestamp = new AmqpTimestamp(message.CreatedAt.ToUnixTimeSeconds()),
        };

        if (message.OrderingKey is not null)
        {
            properties.Headers = new Dictionary<string, object?> { [OrderingKeyHeader] = message.OrderingKey };
        }

        return properties;
    }
}

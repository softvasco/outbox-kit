namespace OutboxKit.Tests.Dispatching;

internal sealed class RecordingTransport : IMessageTransport
{
    private readonly Dictionary<Guid, int> _failuresLeft = [];

    public List<Guid> Sent { get; } = [];

    public void FailOn(OutboxMessage message, int times = int.MaxValue) => _failuresLeft[message.Id] = times;

    public Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        if (_failuresLeft.TryGetValue(message.Id, out var left) && left > 0)
        {
            _failuresLeft[message.Id] = left - 1;
            throw new HttpRequestException("broker unavailable");
        }

        Sent.Add(message.Id);
        return Task.CompletedTask;
    }
}

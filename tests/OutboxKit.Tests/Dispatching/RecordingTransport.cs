namespace OutboxKit.Tests.Dispatching;

internal sealed class RecordingTransport : IMessageTransport
{
    private readonly HashSet<Guid> _failing = [];

    public List<Guid> Sent { get; } = [];

    public void FailOn(OutboxMessage message) => _failing.Add(message.Id);

    public Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        if (_failing.Contains(message.Id))
        {
            throw new HttpRequestException("broker unavailable");
        }

        Sent.Add(message.Id);
        return Task.CompletedTask;
    }
}

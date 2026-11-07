namespace OutboxKit;

public sealed class OutboxDispatcherOptions
{
    /// <summary>How long to wait after a batch that wasn't full before looking again.</summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    public int BatchSize { get; set; } = 100;

    /// <summary>How long a claimed batch stays with this instance. A relay that dies frees its batch after this.</summary>
    public TimeSpan ClaimDuration { get; set; } = TimeSpan.FromSeconds(30);

    public OutboxRetryPolicy Retry { get; set; } = new();

    internal bool IsValid =>
        PollingInterval > TimeSpan.Zero && BatchSize >= 1 && ClaimDuration > TimeSpan.Zero && Retry is { IsValid: true };

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(PollingInterval, TimeSpan.Zero, nameof(PollingInterval));
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ClaimDuration, TimeSpan.Zero, nameof(ClaimDuration));
        ArgumentNullException.ThrowIfNull(Retry);
        Retry.Validate();
    }
}

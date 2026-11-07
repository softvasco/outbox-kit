namespace OutboxKit;

/// <summary>How long a message waits after a failed send, and when it is parked.</summary>
public sealed class OutboxRetryPolicy
{
    public int MaxAttempts { get; init; } = 10;

    public TimeSpan FirstDelay { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMinutes(5);

    internal bool IsValid => MaxAttempts >= 1 && FirstDelay > TimeSpan.Zero && MaxDelay >= FirstDelay;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxAttempts, 1, nameof(MaxAttempts));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(FirstDelay, TimeSpan.Zero, nameof(FirstDelay));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxDelay, FirstDelay, nameof(MaxDelay));
    }

    /// <summary>True when the send that just failed was the last one allowed.</summary>
    public bool IsExhausted(int failedAttempts) => failedAttempts >= MaxAttempts;

    /// <summary>The wait after this many failed sends: doubling, capped at MaxDelay.</summary>
    public TimeSpan DelayAfter(int failedAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(failedAttempts, 1);

        // done in doubles so a large attempt count caps instead of overflowing
        var ticks = FirstDelay.Ticks * Math.Pow(2, failedAttempts - 1);
        return ticks >= MaxDelay.Ticks ? MaxDelay : TimeSpan.FromTicks((long)ticks);
    }
}

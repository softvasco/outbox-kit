namespace OutboxKit;

public sealed class OutboxRetentionOptions
{
    /// <summary>How long a dispatched message stays in the table.</summary>
    public TimeSpan RetainFor { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How long to wait between cleanups.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(1);

    public int BatchSize { get; set; } = 1000;

    internal bool IsValid => RetainFor > TimeSpan.Zero && Interval > TimeSpan.Zero && BatchSize >= 1;
}

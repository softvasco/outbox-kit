namespace OutboxKit.Inbox;

public sealed class InboxRetentionOptions
{
    /// <summary>How long a handled message is remembered. A copy arriving later runs again.</summary>
    public TimeSpan RetainFor { get; set; } = TimeSpan.FromDays(14);

    /// <summary>How long to wait between cleanups.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(1);

    public int BatchSize { get; set; } = 1000;

    internal bool IsValid => RetainFor > TimeSpan.Zero && Interval > TimeSpan.Zero && BatchSize >= 1;
}

namespace OutboxKit.Tests.Dispatching;

public sealed class OutboxRetryPolicyTests
{
    private readonly OutboxRetryPolicy _policy = new() { FirstDelay = TimeSpan.FromSeconds(1), MaxDelay = TimeSpan.FromSeconds(10) };

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(5, 10)]
    [InlineData(500, 10)]
    public void The_wait_doubles_after_each_failure_up_to_the_cap(int failedAttempts, int seconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(seconds), _policy.DelayAfter(failedAttempts));
    }

    [Theory]
    [InlineData(9, false)]
    [InlineData(10, true)]
    [InlineData(11, true)]
    public void A_message_has_ten_sends_by_default(int failedAttempts, bool exhausted)
    {
        Assert.Equal(exhausted, new OutboxRetryPolicy().IsExhausted(failedAttempts));
    }

    [Theory]
    [InlineData(0, 1, 10)]
    [InlineData(3, 0, 10)]
    [InlineData(3, 10, 1)]
    public void Settings_that_never_send_or_never_wait_are_refused(int maxAttempts, int firstSeconds, int maxSeconds)
    {
        var policy = new OutboxRetryPolicy
        {
            MaxAttempts = maxAttempts,
            FirstDelay = TimeSpan.FromSeconds(firstSeconds),
            MaxDelay = TimeSpan.FromSeconds(maxSeconds),
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxDispatcher(new InMemoryOutboxStore(), new RecordingTransport(), TimeProvider.System, policy));
    }
}

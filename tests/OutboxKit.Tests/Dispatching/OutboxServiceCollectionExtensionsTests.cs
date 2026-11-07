using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace OutboxKit.Tests.Dispatching;

public sealed class OutboxServiceCollectionExtensionsTests
{
    [Fact]
    public void The_dispatcher_runs_as_a_hosted_service()
    {
        using var provider = Services().AddOutboxDispatcher().BuildServiceProvider(validateScopes: true);

        Assert.Single(provider.GetServices<IHostedService>().OfType<OutboxDispatcherService>());
    }

    [Fact]
    public void Options_come_from_the_callback()
    {
        using var provider = Services()
            .AddOutboxDispatcher(o => (o.PollingInterval, o.BatchSize) = (TimeSpan.FromSeconds(3), 50))
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<OutboxDispatcherOptions>>().Value;

        Assert.Equal(TimeSpan.FromSeconds(3), options.PollingInterval);
        Assert.Equal(50, options.BatchSize);
    }

    [Fact]
    public void Bad_options_fail_when_resolved_not_on_the_first_poll()
    {
        using var provider = Services().AddOutboxDispatcher(o => o.BatchSize = 0).BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<OutboxDispatcherOptions>>().Value);
    }

    [Fact]
    public void The_retry_policy_from_the_options_reaches_the_dispatcher()
    {
        var retry = new OutboxRetryPolicy { MaxAttempts = 3 };
        using var provider = Services().AddOutboxDispatcher(o => o.Retry = retry).BuildServiceProvider();

        provider.GetRequiredService<OutboxDispatcher>();

        Assert.Same(retry, provider.GetRequiredService<OutboxRetryPolicy>());
    }

    [Fact]
    public void A_retry_policy_that_never_sends_is_refused_on_resolve()
    {
        using var provider = Services().AddOutboxDispatcher(o => o.Retry = new OutboxRetryPolicy { MaxAttempts = 0 }).BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<OutboxDispatcherOptions>>().Value);
    }

    [Fact]
    public void A_time_provider_the_app_already_registered_is_kept()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        using var provider = Services().AddSingleton<TimeProvider>(clock).AddOutboxDispatcher().BuildServiceProvider();

        Assert.Same(clock, provider.GetRequiredService<TimeProvider>());
    }

    private static IServiceCollection Services() =>
        new ServiceCollection()
            .AddLogging()
            .AddSingleton<IOutboxStore, InMemoryOutboxStore>()
            .AddSingleton<IMessageTransport, RecordingTransport>();
}

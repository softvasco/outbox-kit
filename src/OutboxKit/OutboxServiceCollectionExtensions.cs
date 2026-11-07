using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace OutboxKit;

public static class OutboxServiceCollectionExtensions
{
    /// <summary>
    /// Runs the dispatcher as a hosted service. Register an <see cref="IOutboxStore"/> and an <see cref="IMessageTransport"/>
    /// as singletons too: the dispatcher lives as long as the host.
    /// </summary>
    public static IServiceCollection AddOutboxDispatcher(this IServiceCollection services, Action<OutboxDispatcherOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<OutboxDispatcherOptions>()
            .Configure(options => configure?.Invoke(options))
            .Validate(options => options.IsValid, "PollingInterval and ClaimDuration must be positive, BatchSize at least 1, Retry.MaxAttempts at least 1, and 0 < Retry.FirstDelay <= Retry.MaxDelay.")
            .ValidateOnStart();

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(provider => provider.GetRequiredService<IOptions<OutboxDispatcherOptions>>().Value.Retry);
        services.TryAddSingleton<OutboxDispatcher>();
        services.AddHostedService<OutboxDispatcherService>();
        return services;
    }
}

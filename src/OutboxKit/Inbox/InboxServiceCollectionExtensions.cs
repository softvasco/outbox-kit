using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace OutboxKit.Inbox;

public static class InboxServiceCollectionExtensions
{
    /// <summary>Deletes old inbox rows in the background; needs an IInboxRetentionStore.</summary>
    public static IServiceCollection AddInboxRetention(this IServiceCollection services, Action<InboxRetentionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<InboxRetentionOptions>()
            .Configure(options => configure?.Invoke(options))
            .Validate(options => options.IsValid, "RetainFor and Interval must be positive and BatchSize at least 1.")
            .ValidateOnStart();

        services.TryAddSingleton(TimeProvider.System);
        services.AddHostedService<InboxRetentionService>();
        return services;
    }
}

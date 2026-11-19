using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace OutboxKit.AzureServiceBus;

public static class ServiceBusServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="ServiceBusTransport"/> as the outbox transport. It sends through the
    /// <see cref="ServiceBusClient"/> registered in the container, which the app creates and owns.
    /// </summary>
    public static IServiceCollection AddServiceBusTransport(this IServiceCollection services, Action<ServiceBusTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<ServiceBusTransportOptions>()
            .Configure(configure)
            .Validate(options => options.IsValid, "EntityName and ContentType can't be empty and SendTimeout must be positive.")
            .ValidateOnStart();

        services.AddSingleton<IMessageTransport>(provider => new ServiceBusTransport(
            provider.GetRequiredService<ServiceBusClient>(),
            provider.GetRequiredService<IOptions<ServiceBusTransportOptions>>().Value));
        return services;
    }
}

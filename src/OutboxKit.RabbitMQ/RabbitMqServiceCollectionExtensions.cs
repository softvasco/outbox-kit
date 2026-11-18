using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace OutboxKit.RabbitMQ;

public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="RabbitMqTransport"/> as the outbox transport. It publishes on the <see cref="IConnection"/>
    /// registered in the container, which the app creates and owns.
    /// </summary>
    public static IServiceCollection AddRabbitMqTransport(this IServiceCollection services, Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<RabbitMqTransportOptions>()
            .Configure(options => configure?.Invoke(options))
            .Validate(options => options.IsValid, "Exchange and RoutingKey can't be null, ContentType can't be empty and ConfirmTimeout must be positive.")
            .ValidateOnStart();

        services.AddSingleton<IMessageTransport>(provider => new RabbitMqTransport(
            provider.GetRequiredService<IConnection>(),
            provider.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value));
        return services;
    }
}

using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace OutboxKit;

/// <summary>
/// Maps message types to the names stored in the outbox, so a class can be renamed or moved
/// without breaking the messages already saved.
/// </summary>
public sealed class MessageTypeRegistry
{
    private readonly FrozenDictionary<Type, string> _namesByType;
    private readonly FrozenDictionary<string, Type> _typesByName;

    public MessageTypeRegistry(IEnumerable<MessageTypeRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);

        var namesByType = new Dictionary<Type, string>();
        var typesByName = new Dictionary<string, Type>(StringComparer.Ordinal);

        foreach (var registration in registrations)
        {
            ArgumentNullException.ThrowIfNull(registration.ClrType, nameof(registrations));
            ArgumentException.ThrowIfNullOrWhiteSpace(registration.Name, nameof(registrations));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(registration.Name.Length, OutboxMessage.MaxTypeLength, nameof(registrations));

            if (!namesByType.TryAdd(registration.ClrType, registration.Name))
            {
                throw new ArgumentException($"{registration.ClrType.FullName} is registered twice.", nameof(registrations));
            }

            if (!typesByName.TryAdd(registration.Name, registration.ClrType))
            {
                throw new ArgumentException(
                    $"The name '{registration.Name}' is used by both {typesByName[registration.Name].FullName} and {registration.ClrType.FullName}.",
                    nameof(registrations));
            }
        }

        _namesByType = namesByType.ToFrozenDictionary();
        _typesByName = typesByName.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public string GetName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return _namesByType.TryGetValue(type, out var name) ? name : throw UnknownMessageTypeException.For(type);
    }

    public Type GetClrType(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return _typesByName.TryGetValue(name, out var type) ? type : throw UnknownMessageTypeException.For(name);
    }

    public bool TryGetClrType(string name, [NotNullWhen(true)] out Type? type)
    {
        ArgumentNullException.ThrowIfNull(name);
        return _typesByName.TryGetValue(name, out type);
    }
}

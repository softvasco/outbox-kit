namespace OutboxKit;

public interface IMessageSerializer
{
    string Serialize(object message, Type type);

    object Deserialize(string payload, Type type);
}

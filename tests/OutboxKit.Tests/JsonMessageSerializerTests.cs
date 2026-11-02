using System.Text.Json;

namespace OutboxKit.Tests;

public class JsonMessageSerializerTests
{
    private sealed record PaymentReceived(Guid PaymentId, decimal Amount, string Currency);

    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public void Round_trips_a_message_without_losing_decimal_precision()
    {
        var payment = new PaymentReceived(Guid.CreateVersion7(), 1234567.8901m, "EUR");

        var payload = _serializer.Serialize(payment, typeof(PaymentReceived));
        var back = _serializer.Deserialize(payload, typeof(PaymentReceived));

        Assert.Equal(payment, back);
    }

    [Fact]
    public void Uses_camel_case_names_by_default()
    {
        var payload = _serializer.Serialize(new PaymentReceived(Guid.Empty, 10m, "EUR"), typeof(PaymentReceived));

        Assert.Contains("\"amount\":10", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void Honours_the_options_it_is_given()
    {
        var serializer = new JsonMessageSerializer(new JsonSerializerOptions());

        var payload = serializer.Serialize(new PaymentReceived(Guid.Empty, 10m, "EUR"), typeof(PaymentReceived));

        Assert.Contains("\"Amount\":10", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void A_null_payload_fails_instead_of_returning_null()
    {
        Assert.Throws<JsonException>(() => _serializer.Deserialize("null", typeof(PaymentReceived)));
    }
}

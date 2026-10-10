using System.Data.SqlTypes;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

public class ProducerAsync
{
  private readonly IProducer<string, string> _producer;

  public ProducerAsync(IProducer<string, string> producer)
  {
    _producer = producer;
  }

  public async Task SendMessage(string key, object message)
  {
    var val = new Message<string, string>
    {
      Key = key,
      Value = JsonSerializer.Serialize(message),
      Headers = new Headers { { "event-type", Encoding.UTF8.GetBytes("OrderCreated") } }
    };

    var res = await _producer.ProduceAsync("order-event", val);
    System.Console.WriteLine($"Partition:{res.Partition.Value}, offset {res.Offset.Value}");
  }
}
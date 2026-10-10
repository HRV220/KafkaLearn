using Confluent.Kafka;
using Microsoft.VisualBasic;

public class ConsumerWorker : BackgroundService
{
  private readonly ConsumerConfig _config = new()
  {
    BootstrapServers = "localhost:9092",
    GroupId = "test",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
    EnableAutoOffsetStore = false
  };

  protected override Task ExecuteAsync(CancellationToken stoppingToken)
  {
    return Task.Run(() => Run(stoppingToken), stoppingToken);
  }

  private void Run(CancellationToken ct)
  {
    using var consumer = new ConsumerBuilder<string, string>(_config).Build();
    consumer.Subscribe("order-event");
    try
    {
      while (!ct.IsCancellationRequested)
      {
        var res = consumer.Consume(ct);

        System.Console.WriteLine($"{res.Topic} {res.Partition.Value} {res.Offset.Value} {res.Message.Key} {res.Message.Value}");

        consumer.StoreOffset(res);
      }
    }
    catch (OperationCanceledException)
    {

    }
    finally
    {
      consumer.Close();
    }
  }
}
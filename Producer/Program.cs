using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

string[] customers = ["amine", "richard", "carl"];
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        Acks = Acks.All,
        EnableIdempotence = true
    };
    return new ProducerBuilder<string, string>(config).Build();
});
builder.Services.AddSingleton<ProducerAsync>();
var app = builder.Build();
var producer = app.Services.GetRequiredService<ProducerAsync>();
for (var i = 1; i <= 12; i++)
{
    var customer = customers[i % customers.Length];
    var order = new { OrderId = i, Customer = customer, Amount = 100 * i };
    await producer.SendMessage(customer, order);
}
app.Run();
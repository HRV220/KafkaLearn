var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<ConsumerWorker>();
var app = builder.Build();
app.Run();


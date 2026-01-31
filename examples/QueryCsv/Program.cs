using DataFusionSharp;

await using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

var ordersCsvFilePath = Path.Combine("Data", "orders.csv");
await context.RegisterCsvAsync("orders", ordersCsvFilePath);

using var df = await context.SqlAsync("SELECT customer_id, sum(amount) FROM orders WHERE status = 'completed' GROUP BY customer_id");

await df.ShowAsync();

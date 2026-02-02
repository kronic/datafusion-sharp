using Apache.Arrow;
using DataFusionSharp;

await using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

await context.RegisterCsvAsync("orders", Path.Combine("Data", "orders.csv"));

using var df = await context.SqlAsync(
    @"SELECT customer_id, sum(amount) AS total_amount
      FROM orders
      WHERE status = 'completed'
      GROUP BY customer_id ORDER BY customer_id");

Console.WriteLine("=== Query Results ===");
Console.WriteLine(await df.ToStringAsync());
Console.WriteLine($"Total rows: {await df.CountAsync()}");

Console.WriteLine("=== Schema ===");
var schema = await df.GetSchemaAsync();
foreach (var field in schema.FieldsList)
    Console.WriteLine($"  {field.Name}: {field.DataType}");

Console.WriteLine("=== Collected Data ===");
var collectedData = await df.CollectAsync();
foreach (var batch in collectedData.Batches)
{
    for (var r = 0; r < batch.Length; r++)
    {
        for (var c = 0; c < batch.ColumnCount; c++)
        {
            var v = batch.Column(c) switch
            {
                Int64Array a => (object)a.Values[r],
                DoubleArray a => a.Values[r],
                _ => null
            };
            Console.Write($"{v}\t");
        }
        Console.WriteLine();
    }
}

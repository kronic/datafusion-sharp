using Apache.Arrow;
using DataFusionSharp;

await using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

await context.RegisterCsvAsync("customers", Path.Combine("Data", "customers.csv"));
await context.RegisterCsvAsync("orders", Path.Combine("Data", "orders.csv"));

using var df = await context.SqlAsync(
    """
    SELECT
        c.customer_name,
        sum(o.order_amount) AS total_amount
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'Completed'
    GROUP BY c.customer_name
    ORDER BY c.customer_name
    """);

Console.WriteLine("=== Query Results ===");
Console.WriteLine(await df.ToStringAsync());
Console.WriteLine($"Total rows: {await df.CountAsync()}");
Console.WriteLine();

Console.WriteLine("=== Schema ===");
var schema = await df.GetSchemaAsync();
foreach (var field in schema.FieldsList)
    Console.WriteLine($"  {field.Name}: {field.DataType}");
Console.WriteLine();

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
                StringArray a => (object)a.GetString(r),
                Int64Array a => a.GetValue(r),
                _ => null
            };
            Console.Write($"{v}\t");
        }
        Console.WriteLine();
    }
}

using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "dotnet-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("cross-platform-topic");

            Console.WriteLine("Consumer .NET iniciado. Aguardando mensagens...");

            try
            {
                while (true)
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (result != null)
                    {
                        Console.WriteLine($"[Partição: {result.Partition}] Key: {result.Message.Key} | Valor: {result.Message.Value}");
                    }
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Erro: {e.Error.Reason}");
            }
        }
    }
}
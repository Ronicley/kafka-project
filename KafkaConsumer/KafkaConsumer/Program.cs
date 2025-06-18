using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaConsumer
{
    [DataContract(Name = "User", Namespace = "com.meudominio.models")]
    public class User
    {
        [DataMember(Name = "email")]
        public string Email { get; set; }

        [DataMember(Name = "name")]
        public string Name { get; set; }

        [DataMember(Name = "age")]
        public int Age { get; set; }
    }

    class Program
    {
        static void Main()
        {
            var schemaConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers    = "localhost:9092",
                GroupId             = "user-consumer-group",
                AutoOffsetReset     = AutoOffsetReset.Earliest,
                EnableAutoCommit    = false
            };

            using var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

            // Desserializador para GenericRecord
            var genericDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry)
                                          .AsSyncOverAsync();

            using var consumer = new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                .SetValueDeserializer(genericDeserializer)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();

            consumer.Subscribe("users");
            Console.WriteLine("Consumer iniciado. Aguardando mensagens...");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(cts.Token);
                        var record = result.Message.Value!;  // GenericRecord

                        // Mapeia para sua classe POCO User usando o indexador e cast explícito
                        var user = new User
                        {
                            Email = (string) record["email"],
                            Name  = (string) record["name"],
                            Age   = (int)    record["age"]
                        };

                        Console.WriteLine($"Received: {user.Email}, {user.Name}, {user.Age}");
                        consumer.Commit(result);
                    }
                    catch (ConsumeException e) when (e.Error.Code == ErrorCode.Local_ValueDeserialization)
                    {
                        Console.WriteLine($"❗ Erro de desserialização: {e.Error.Reason}");

                        var raw = e.ConsumerRecord.Value;
                        if (raw != null && raw.Length > 5)
                        {
                            try
                            {
                                // Extrai schema ID em big‑endian
                                int networkOrderId = BitConverter.ToInt32(raw, 1);
                                int schemaId = IPAddress.NetworkToHostOrder(networkOrderId);
                                Console.WriteLine($"Schema ID (corrigido): {schemaId}");

                                var schema = schemaRegistry.GetSchemaAsync(schemaId).Result;
                                Console.WriteLine($"Schema: {schema.SchemaString}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"⛔ Erro no diagnóstico: {ex.Message}");
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl+C pressionado — sai graciosamente
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}

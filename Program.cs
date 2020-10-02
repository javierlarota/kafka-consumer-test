using System;
using System.Threading;
using Confluent.Kafka;

namespace kafkatest
{    
    class Program
    {
        static void Main(string[] args)
        {
            var topic = args[0];

            var consumerConfig= new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Latest // Start reading messages from now
            };

            Console.WriteLine($"======================================");
            Console.WriteLine($"Consuming Messages from topic: {topic}");
            Console.WriteLine($"======================================");

            using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                consumerBuilder.Subscribe(topic);
    
                var cancellationToken = new CancellationTokenSource();
                
                // Capture the event if CTRL+C is pressed in the console.
                Console.CancelKeyPress += (obj, e) => { 
                    e.Cancel = true; 
                    cancellationToken.Cancel();
                };
    
                try
                {
                    while (true)
                    {
                        try
                        {
                            var result = consumerBuilder.Consume(cancellationToken.Token);
                            Console.WriteLine($"Message: '{result.Message.Value}'");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"UnExpected Exception: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumerBuilder.Close();
                }

                Console.WriteLine("Consumer stopped successfully.");
            }
        }
    }
}

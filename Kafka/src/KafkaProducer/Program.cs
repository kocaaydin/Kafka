using Confluent.Kafka;
using System;

namespace KafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var topicName = "order";
            var kafkaUrl = "127.0.0.1:9092";

            var config = new ProducerConfig() { BootstrapServers = kafkaUrl, Partitioner = Partitioner.Random };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                for (int i = 0; i < 100000; i++)
                {
                    var order = new {
                        OrderId = Guid.NewGuid(),
                        Date = DateTime.Now
                    };

                    string m = Newtonsoft.Json.JsonConvert.SerializeObject(order, Newtonsoft.Json.Formatting.Indented);

                    Message<string, string> message = new Message<string, string> { Key = order.OrderId.ToString(), Value = m };

                    var result = producer.ProduceAsync(topicName, message).Result;
                    Console.WriteLine($"{result.TopicPartitionOffset}{result.Partition}'");
                }
            }

            Console.ReadKey();
        }
    }
}

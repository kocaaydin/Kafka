using Confluent.Kafka;
using System;

namespace KafkaConsumerConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("KafkaConsumer");
            var consumergroup = "order-consumer";
            var topicName = "order";
            var brokerList = "127.0.0.1:9092";
            var partition = 0;
           

            var config = new ConsumerConfig { GroupId = consumergroup, BootstrapServers = brokerList };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                TopicPartition topicPartition = new TopicPartition(topicName, partition);

                Console.WriteLine($"Bağlanılan Topic {topicName} Partition -> {partition} ");

                consumer.Subscribe(topicName);
                while (true)
                {
                    ConsumeResult<string, string> consumeResult = consumer.Consume();
                    Console.WriteLine($"Mesaj Geldi: Partition : {consumeResult.Partition} Offset : {consumeResult.TopicPartitionOffset}: Content : {consumeResult.Message.Value}");
                }
            }
        }
    }
}

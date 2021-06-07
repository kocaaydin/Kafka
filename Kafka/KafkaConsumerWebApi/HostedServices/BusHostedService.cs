using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumerWebApi.HostedServices
{
    public class BusHostedService : BackgroundService
    {
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task.Run(() => TestMethod());

            return Task.CompletedTask;
        }

        private void TestMethod()
        {
            var consumergroup = "sale-consumer";
            var topicName = "order";
            var brokerList = "127.0.0.1:9092";
            var partition = 0;

            var config = new ConsumerConfig { GroupId = consumergroup, BootstrapServers = brokerList };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                TopicPartition topicPartition = new TopicPartition(topicName, partition);

                consumer.Subscribe(topicName);
                while (true)
                {
                    ConsumeResult<string, string> consumeResult = consumer.Consume();
                    Debug.WriteLine($"Mesaj Geldi: Partition : {consumeResult.Partition} Offset : {consumeResult.TopicPartitionOffset}: Content : {consumeResult.Message.Value}");
                }
            }
        }
    }
}

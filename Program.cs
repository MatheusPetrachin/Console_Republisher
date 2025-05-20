using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Configuration;
using System.Text;

class Program
{
    static void Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

        var factory = new ConnectionFactory
        {
            HostName = configuration["RabbitMQ:HostName"],
            UserName = configuration["RabbitMQ:UserName"],
            Password = configuration["RabbitMQ:Password"]
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var sourceQueue = configuration["RabbitMQ:SourceQueue"];
        var targetQueue = configuration["RabbitMQ:TargetQueue"];
        var exchangeName = configuration["RabbitMQ:ExchangeName"];

        // Declare queues
        channel.QueueDeclare(sourceQueue, true, false, false);
        channel.QueueDeclare(targetQueue, true, false, false);

        Console.WriteLine($"Starting to listen on queue: {sourceQueue}");
        Console.WriteLine($"Messages will be republished to: {targetQueue}");

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            try
            {
                // Republish the message to the target queue
                channel.BasicPublish(
                    exchange: exchangeName,
                    routingKey: targetQueue,
                    basicProperties: null,
                    body: body);

                // Acknowledge the message from the source queue
                channel.BasicAck(ea.DeliveryTag, false);

                Console.WriteLine($"Message republished successfully: {message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error republishing message: {ex.Message}");
                // Negative acknowledge the message to retry
                channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        // Start consuming messages
        channel.BasicConsume(
            queue: sourceQueue,
            autoAck: false,
            consumer: consumer);

        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }
}

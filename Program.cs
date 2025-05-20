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
            Uri = new Uri(configuration["RabbitMQ:Uri"]!)
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var sourceQueue = configuration["RabbitMQ:SourceQueue"];
        var targetQueue = configuration["RabbitMQ:TargetQueue"];
        var exchangeName = configuration["RabbitMQ:ExchangeName"];

        Console.WriteLine($"Connecting to exchange: {exchangeName}");
        Console.WriteLine($"Source queue: {sourceQueue}");
        Console.WriteLine($"Target queue: {targetQueue}");

        // Queue arguments
        var dlqArgs = new Dictionary<string, object>
        {
            { "x-message-ttl", 60000 }, // 60 segundos TTL
            { "x-queue-type", "classic" }
        };

        var mainQueueArgs = new Dictionary<string, object>
        {
            { "x-queue-type", "classic" }
        };

        try
        {
            // Verificar se as filas existem antes de tentar declarar
            channel.QueueDeclarePassive(sourceQueue);
            channel.QueueDeclarePassive(targetQueue);

            // Garantir que a fila de destino está vinculada ao exchange
            channel.QueueBind(targetQueue, exchangeName, targetQueue);

            Console.WriteLine("Queues verified and bound successfully");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error setting up queues: {ex.Message}");
            return;
        }

        Console.WriteLine($"Starting to listen on queue: {sourceQueue}");
        Console.WriteLine($"Messages will be republished to: {targetQueue}");

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine($"Received message: {message}");

            try
            {
                // Republish the message to the target queue
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2; // mensagem persistente
                properties.Headers = ea.BasicProperties.Headers; // preserva os headers originais

                channel.BasicPublish(
                    exchange: "",  // Publicando diretamente na fila
                    routingKey: targetQueue,
                    basicProperties: properties,
                    body: body);

                // Acknowledge the message from the source queue
                channel.BasicAck(ea.DeliveryTag, false);

                Console.WriteLine($"Message republished successfully to queue: {targetQueue}");

                // Verificar quantidade de mensagens na fila de destino
                var targetQueueDeclare = channel.QueueDeclarePassive(targetQueue);
                Console.WriteLine($"Messages in target queue: {targetQueueDeclare.MessageCount}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error republishing message: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                // Negative acknowledge the message to retry
                channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        // Start consuming messages with prefetch count
        channel.BasicQos(0, 15, false); // processa 15 mensagens por vez
        channel.BasicConsume(
            queue: sourceQueue,
            autoAck: false,
            consumer: consumer);

        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }
}

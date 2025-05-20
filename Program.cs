using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Collections.Generic;

class Program
{
    static void Main(string[] args)
    {
        // Carrega configurações do appsettings.json
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var uri = configuration["RabbitMQ:Uri"];
        var sourceQueue = configuration["RabbitMQ:SourceQueue"];
        var targetQueue = configuration["RabbitMQ:TargetQueue"];
        var exchangeName = configuration["RabbitMQ:ExchangeName"];

        // Valida configurações obrigatórias
        if (string.IsNullOrEmpty(uri) || string.IsNullOrEmpty(sourceQueue) || string.IsNullOrEmpty(targetQueue))
        {
            Console.WriteLine("Configuração inválida. Verifique RabbitMQ:Uri, SourceQueue e TargetQueue.");
            return;
        }

        var factory = new ConnectionFactory
        {
            Uri = new Uri(uri)
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        Console.WriteLine($"Conectado ao RabbitMQ.");
        Console.WriteLine($"Source Queue: {sourceQueue}");
        Console.WriteLine($"Target Queue: {targetQueue}");
        Console.WriteLine($"Exchange (se aplicável): {exchangeName}");

        try
        {
            // Verifica se as filas existem
            channel.QueueDeclarePassive(sourceQueue);
            channel.QueueDeclarePassive(targetQueue);

            if (!string.IsNullOrEmpty(exchangeName))
            {
                // Se o exchange foi definido, faz o bind da fila de destino
                channel.QueueBind(targetQueue, exchangeName, targetQueue);
            }

            Console.WriteLine("Filas verificadas e vinculadas com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao configurar as filas: {ex.Message}");
            return;
        }

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine($"Mensagem recebida: {message}");

            try
            {
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2; // Persistente
                properties.Headers = ea.BasicProperties?.Headers;

                // Publica no exchange especificado ou diretamente na fila
                channel.BasicPublish(
                    exchange: "", // Publica direto na fila e não no exchange
                    routingKey: targetQueue,
                    basicProperties: properties,
                    body: body
                );

                // Acknowledgement
                channel.BasicAck(ea.DeliveryTag, false);
                Console.WriteLine($"Mensagem publicada em: {targetQueue}");

                // Opcional: Mostra contagem atual na fila de destino
                var targetInfo = channel.QueueDeclarePassive(targetQueue);
                Console.WriteLine($"Mensagens na fila de destino: {targetInfo.MessageCount}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao republicar mensagem: {ex.Message}");
                channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        // Define o prefetch para limitar a quantidade de mensagens não confirmadas
        channel.BasicQos(0, 15, false);

        // Inicia o consumo da fila
        channel.BasicConsume(
            queue: sourceQueue,
            autoAck: false,
            consumer: consumer
        );

        Console.WriteLine("Pressione [Enter] para sair...");
        Console.ReadLine();
    }
}

using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;


using ILoggerFactory loggerFactory =
    LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "HH:mm:ss ";
            });
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        }
    );

// Define the logger for the StreamSystem and the Producer/Consumer
var producerLogger = loggerFactory.CreateLogger<Producer>();
var consumerLogger = loggerFactory.CreateLogger<Consumer>();
var streamLogger = loggerFactory.CreateLogger<StreamSystem>();

var streamSystem = await
    StreamSystem.Create(
            new StreamSystemConfig()
            {
                UserName = "admin",
                Password = "pass.123",
                Endpoints = new List<EndPoint>()
                {
                    new IPEndPoint(IPAddress.Loopback, 5552),
                    new IPEndPoint(IPAddress.Loopback, 5553),
                    new IPEndPoint(IPAddress.Loopback, 5554)
                }
            },
            streamLogger
        )
        .ConfigureAwait(false);

const string streamName = "test-streams";

await ConsumeStreams();


async Task ConsumeStreams()
{
    Console.WriteLine("Starting consuming...");
    var consumer = await Consumer.Create(
            new ConsumerConfig(streamSystem, streamName)
            {
                OffsetSpec = new OffsetTypeFirst(), 
                MessageHandler = async (sourceStream, consumer, messageContext, message) =>
                {
                    Console.WriteLine(
                        $"Received message: {Encoding.ASCII.GetString(message.Data.Contents)} |{messageContext.Offset} | {messageContext.Timestamp.TotalMilliseconds}");
                    await Task.CompletedTask.ConfigureAwait(false);
                }
            },
            consumerLogger
        )
        .ConfigureAwait(false);

    Console.ReadKey();
}


const int messageCount = 10; 

var producer = await CreateProducer();

await ProduceMessage();

await producer.Close().ConfigureAwait(false);

async Task<Producer> CreateProducer()
{
    var producer1 = await
        Producer.Create(
                new ProducerConfig(streamSystem, streamName)
                {
                    // ReconnectStrategy = null,
                    // Reference = null,
                    // //ConfirmationHandler = null,
                    // ClientProvidedName = null,
                    // Filter = null,
                    // MaxInFlight = 0,
                    // MessagesBufferSize = 0,
                    // SuperStreamConfig = null,
                    TimeoutMessageAfter = TimeSpan.FromSeconds(1),
                    ConfirmationHandler = async confirmation =>
                    {
                        switch (confirmation.Status)
                        {
                            case ConfirmationStatus.Confirmed:
                                Console.WriteLine(
                                    $"Message {confirmation.PublishingId} confirmed @{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");
                                break;
                            case ConfirmationStatus.StreamNotAvailable:
                            case ConfirmationStatus.InternalError:
                            case ConfirmationStatus.AccessRefused:
                            case ConfirmationStatus.PreconditionFailed:
                            case ConfirmationStatus.PublisherDoesNotExist:
                            case ConfirmationStatus.UndefinedError:
                            case ConfirmationStatus.ClientTimeoutError:
                                Console.WriteLine(
                                    $"Message {confirmation.PublishingId} failed with {confirmation.Status}");
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                        await Task.CompletedTask.ConfigureAwait(false);
                    }
                },
                producerLogger
            )
            .ConfigureAwait(false);
    return producer1;
}

async Task ProduceMessage()
{
    Console.WriteLine("Starting publishing...");
    for (var i = 0; i < messageCount; i++)
    {
        await producer.Send(
            new Message(Encoding.ASCII.GetBytes($"{i} @ {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"))
        ).ConfigureAwait(false);
        await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
    }
}
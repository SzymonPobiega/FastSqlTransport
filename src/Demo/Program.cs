using System;
using System.Linq;
using System.Threading.Tasks;

namespace Demo
{
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Threading;
    using NServiceBus;
    using NServiceBus.Features;
    using NServiceBus.Transport.SQLServer;

    class Program
    {
        public const int MessageCount = 120000;
        static int messagesSent;
        public static TaskCompletionSource<bool> receivingComplete = new TaskCompletionSource<bool>();

        static void Main(string[] args)
        {
            Start().GetAwaiter().GetResult();
        }

        static async Task Start()
        {
            //Console.WriteLine("Please enter catalog name");
            //var catalog = Console.ReadLine();
            var catalog = "nservicebus";

            var senderConfig = new EndpointConfiguration("Sender");
            senderConfig.SendFailedMessagesTo("error");
            senderConfig.EnableInstallers();
            senderConfig.UsePersistence<InMemoryPersistence>();
            senderConfig.SendOnly();

            var transport = senderConfig.UseTransport<SqlServerTransport>();
            transport.Transactions(TransportTransactionMode.SendsAtomicWithReceive);
            var connectionString = $"Data Source=(local);Initial Catalog={catalog};Integrated Security=True";
            transport.ConnectionString(connectionString);
            transport.Routing().RouteToEndpoint(typeof(MyMessage), "Receiver");

            //Make sure queue is created
            var receiver = await Endpoint.Start(CreateReceiverConfiguration(connectionString));
            await receiver.Stop();

            var sender = await Endpoint.Start(senderConfig);

            //Console.WriteLine("Press <enter> to start the benchmark");
            //Console.ReadLine();

            //Seed the queue
            var sendWatch = Stopwatch.StartNew();
            var sendTask = Send(connectionString, sender).ContinueWith(t =>
            {
                sendWatch.Stop();
                Console.WriteLine($"Sending {messagesSent} took {sendWatch.ElapsedMilliseconds}.");
                if (t.IsFaulted)
                {
                    Console.WriteLine(t.Exception.Flatten());
                }
            });


            //Console.WriteLine("Press <enter> to start receiving");
            //Console.ReadLine();

            //Start receiving
            receiver = await Endpoint.Start(CreateReceiverConfiguration(connectionString));

            //Wait for send to complete
            await sendTask;

            await receivingComplete.Task.ConfigureAwait(false);
            await receiver.Stop().ConfigureAwait(false);
            await sender.Stop();

            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
            await receiver.Stop();
        }

        static EndpointConfiguration CreateReceiverConfiguration(string connectionString)
        {
            var receiverConfig = new EndpointConfiguration("Receiver");
            receiverConfig.SendFailedMessagesTo("error");
            receiverConfig.EnableInstallers();
            receiverConfig.UsePersistence<InMemoryPersistence>();

            var transport = receiverConfig.UseTransport<SqlServerTransport>();
            transport.Transactions(TransportTransactionMode.SendsAtomicWithReceive);
            transport.ConnectionString(connectionString);
            receiverConfig.LimitMessageProcessingConcurrencyTo(8);
            receiverConfig.DisableFeature<TimeoutManager>();
            receiverConfig.Recoverability().DisableLegacyRetriesSatellite();
            return receiverConfig;
        }

        static Task Send(string connectionString, IMessageSession sender)
        {
            var tasks = Enumerable.Range(0, 8)
                .Select(i => Task.Run(() => SendTask(connectionString, sender)))
                .ToArray();

            return Task.WhenAll(tasks);
        }

        static async Task SendTask(string connectionString, IMessageSession sender)
        {
            var options = new SendOptions();

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);
                options.UseFastSending(new ProgressiveDelayQueueFullHandling(5, 10), connection);
                while (true)
                {
                    try
                    {

                        await sender.Send(new MyMessage(), options).ConfigureAwait(false);
                        var sent = Interlocked.Increment(ref messagesSent);
                        if (sent >= MessageCount)
                        {
                            return;
                        }
                    }
                    catch (Exception e)
                    {
                        if (Volatile.Read(ref messagesSent) == MessageCount)
                        {
                            return;
                        }
                        Console.WriteLine(e);
                    }
                }
            }
        }
    }

    class MyMessageHandler : IHandleMessages<MyMessage>
    {
        static Stopwatch watch;
        static int numberOfMessages;

        public Task Handle(MyMessage message, IMessageHandlerContext context)
        {
            var n = Interlocked.Increment(ref numberOfMessages);
            if (n == 1)
            {
                watch = Stopwatch.StartNew();
            }
            else if (n == Program.MessageCount)
            {
                watch.Stop();
                Console.WriteLine("Receiving done in {0}", watch.ElapsedMilliseconds);
                Program.receivingComplete.SetResult(true);
            }
            return Task.FromResult(0);
        }
    }

    class MyMessage : IMessage
    {
    }
}

namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Logging;

    class MessagePump : IPushMessages
    {
        public MessagePump(Func<TransportTransactionMode, ReceiveWithNativeTransaction> receiveStrategyFactory, Func<TableBasedQueueFactory> queueFactoryFactory, IPurgeQueues queuePurger, SqlConnectionFactory connectionFactory, Func<IQueueEmptyHandling> queueEmptyHandlingFactory, QueueAddressTranslator addressTranslator, TimeSpan waitTimeCircuitBreaker)
        {
            this.receiveStrategyFactory = receiveStrategyFactory;
            this.queueFactoryFactory = queueFactoryFactory;
            this.queuePurger = queuePurger;
            this.connectionFactory = connectionFactory;
            this.queueEmptyHandlingFactory = queueEmptyHandlingFactory;
            this.addressTranslator = addressTranslator;
            this.waitTimeCircuitBreaker = waitTimeCircuitBreaker;
        }

        public async Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
        {
            receiveStrategy = receiveStrategyFactory(settings.RequiredTransactionMode);

            receiveCircuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("Receive", waitTimeCircuitBreaker, ex => criticalError.Raise("Failed to receive from " + settings.InputQueue, ex));
            loopCircuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("Receive Loop", waitTimeCircuitBreaker, ex => criticalError.Raise("Failed to run receive loop for " + settings.InputQueue, ex));

            receiveStrategy.Init(onMessage, onError, criticalError);

            inputQueueName = settings.InputQueue;
            errorQueueName = settings.ErrorQueue;

            if (settings.PurgeOnStartup)
            {
                try
                {
                    var inputQueue = GetQueue(settings.InputQueue, queueFactoryFactory());
                    var purgedRowsCount = await queuePurger.Purge(inputQueue).ConfigureAwait(false);

                    Logger.InfoFormat("{0:N} messages purged from queue {1}", purgedRowsCount, settings.InputQueue);
                }
                catch (Exception ex)
                {
                    Logger.Warn("Failed to purge input queue on startup.", ex);
                }
            }
        }

        TableBasedQueue GetQueue(string address, TableBasedQueueFactory factory)
        {
            return factory.Get(addressTranslator.Parse(address).QualifiedTableName, address);
        }

        public void Start(PushRuntimeSettings limitations)
        {
            mainCancellationTokenSource = new CancellationTokenSource();

            receiveTasks = Enumerable.Range(0, limitations.MaxConcurrency)
                .Select(i => Task.Run(() => ProcessMessages(mainCancellationTokenSource.Token), CancellationToken.None))
                .ToArray();
        }

        public async Task Stop()
        {
            const int timeoutDurationInSeconds = 30;
            mainCancellationTokenSource.Cancel();

            // ReSharper disable once MethodSupportsCancellation
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(timeoutDurationInSeconds));
            var finishedTask = await Task.WhenAny(Task.WhenAll(receiveTasks), timeoutTask).ConfigureAwait(false);
            if (finishedTask.Equals(timeoutTask))
            {
                Logger.ErrorFormat($"The message pump failed to stop within the time allowed ({timeoutDurationInSeconds} s)");
            }
            mainCancellationTokenSource.Dispose();
            receiveTasks = null;
        }

        async Task ProcessMessages(CancellationToken stopToken)
        {
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    var factory = queueFactoryFactory();

                    var inputQueue = GetQueue(inputQueueName, factory);
                    var errorQueue = GetQueue(errorQueueName, factory); 

                    await InnerProcessMessages(stopToken, queueEmptyHandlingFactory(), factory, inputQueue, errorQueue).ConfigureAwait(false);
                    loopCircuitBreaker.Success();
                }
                catch (OperationCanceledException)
                {
                    // For graceful shutdown purposes
                }
                catch (SqlException e) when (stopToken.IsCancellationRequested)
                {
                    Logger.Debug("Exception thrown during cancellation", e);
                }
                catch (Exception ex)
                {
                    Logger.Error("Sql Message pump failed", ex);
                    await loopCircuitBreaker.Failure(ex).ConfigureAwait(false);
                }
            }
        }

        async Task InnerProcessMessages(CancellationToken stopToken, IQueueEmptyHandling queueEmptyHandling, TableBasedQueueFactory queueFactory, TableBasedQueue inputQueue, TableBasedQueue errorQueue)
        {
            while (!stopToken.IsCancellationRequested)
            {
                // We cannot dispose this token source because of potential race conditions of concurrent receives
                var loopCancellationTokenSource = new CancellationTokenSource();

                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    while (true)
                    {
                        await InnerReceive(connection, loopCancellationTokenSource, queueFactory, inputQueue, errorQueue).ConfigureAwait(false);
                        if (loopCancellationTokenSource.IsCancellationRequested)
                        {
                            break;
                        }
                        queueEmptyHandling.OnQueueNonEmpty();
                    }
                }
                await queueEmptyHandling.HandleQueueEmpty(stopToken).ConfigureAwait(false);
            }
        }

        async Task InnerReceive(SqlConnection connection, CancellationTokenSource loopCancellationTokenSource, TableBasedQueueFactory queueFactory, TableBasedQueue inputQueue, TableBasedQueue errorQueue)
        {
            try
            {
                await receiveStrategy.ReceiveMessage(inputQueue, errorQueue, queueFactory, connection, loopCancellationTokenSource)
                    .ConfigureAwait(false);

                receiveCircuitBreaker.Success();
            }
            catch (SqlException e) when (e.Number == 1205)
            {
                //Receive has been victim of a lock resolution
                Logger.Warn("Sql receive operation failed.", e);
            }
            catch (Exception ex)
            {
                Logger.Warn("Sql receive operation failed", ex);

                await receiveCircuitBreaker.Failure(ex).ConfigureAwait(false);
            }
        }

        Task[] receiveTasks;
        Func<TransportTransactionMode, ReceiveWithNativeTransaction> receiveStrategyFactory;
        readonly Func<TableBasedQueueFactory> queueFactoryFactory;
        IPurgeQueues queuePurger;
        SqlConnectionFactory connectionFactory;
        Func<IQueueEmptyHandling> queueEmptyHandlingFactory;
        QueueAddressTranslator addressTranslator;
        TimeSpan waitTimeCircuitBreaker;
        CancellationTokenSource mainCancellationTokenSource;
        RepeatedFailuresOverTimeCircuitBreaker receiveCircuitBreaker;
        RepeatedFailuresOverTimeCircuitBreaker loopCircuitBreaker;
        ReceiveWithNativeTransaction receiveStrategy;
        string inputQueueName;
        string errorQueueName;

        static ILog Logger = LogManager.GetLogger<MessagePump>();
    }
}

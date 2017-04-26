namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using Extensibility;
    using Transport;

    class MessageDispatcher : IDispatchMessages
    {
        public MessageDispatcher(QueueAddressTranslator addressTranslator, SqlConnectionFactory connectionFactory, Func<TableBasedQueueFactory> queueFactoryFactory)
        {
            this.addressTranslator = addressTranslator;
            this.connectionFactory = connectionFactory;
            this.queueFactoryFactory = queueFactoryFactory;
        }

        // We need to check if we can support cancellation in here as well?
        public async Task Dispatch(TransportOperations operations, TransportTransaction transportTransaction, ContextBag context)
        {
            var timer = Stopwatch.StartNew();
            TableBasedQueueFactory queueFactory;
            if (!transportTransaction.TryGet(out queueFactory))
            {
                queueFactory = queueFactoryFactory();
            }

            await DeduplicateAndDispatch(operations, ops => DispatchAsIsolated(ops, queueFactory), DispatchConsistency.Isolated).ConfigureAwait(false);
            await DeduplicateAndDispatch(operations, ops => DispatchAsNonIsolated(ops, transportTransaction, queueFactory), DispatchConsistency.Default).ConfigureAwait(false);

            timer.Stop();
            if (timer.ElapsedMilliseconds > 50)
            {
                Console.Write("U");
            }
        }

        Task DeduplicateAndDispatch(TransportOperations operations, Func<List<UnicastTransportOperation>, Task> dispatchMethod, DispatchConsistency dispatchConsistency)
        {
            var operationsToDispatch = operations.UnicastTransportOperations
                .Where(o => o.RequiredDispatchConsistency == dispatchConsistency)
                .GroupBy(o => new DeduplicationKey(o.Message.MessageId, addressTranslator.Parse(o.Destination).Address))
                .Select(g => g.First())
                .ToList();

            return dispatchMethod(operationsToDispatch);
        }

        async Task DispatchAsIsolated(List<UnicastTransportOperation> operations, TableBasedQueueFactory queueFactory)
        {
            if (operations.Count == 0)
            {
                return;
            }
            using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled))
            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            {
                await Send(operations, queueFactory, connection, null).ConfigureAwait(false);

                scope.Complete();
            }
        }

        async Task DispatchAsNonIsolated(List<UnicastTransportOperation> operations, TransportTransaction transportTransaction, TableBasedQueueFactory queueFactory)
        {
            if (operations.Count == 0)
            {
                return;
            }

            if (InReceiveOnlyTransportTransactionMode(transportTransaction))
            {
                await DispatchOperationsWithNewConnectionAndTransaction(operations, queueFactory).ConfigureAwait(false);
                return;
            }

            await DispatchUsingReceiveConnectionOrTransaction(transportTransaction, operations, queueFactory).ConfigureAwait(false);
        }


        async Task DispatchOperationsWithNewConnectionAndTransaction(List<UnicastTransportOperation> operations, TableBasedQueueFactory queueFactory)
        {
            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            {
                if (operations.Count == 1)
                {
                    await Send(operations, queueFactory, connection, null).ConfigureAwait(false);
                    return;
                }

                using (var transaction = connection.BeginTransaction())
                {
                    await Send(operations, queueFactory, connection, transaction).ConfigureAwait(false);
                    transaction.Commit();
                }
            }
        }

        async Task DispatchUsingReceiveConnectionOrTransaction(TransportTransaction transportTransaction, List<UnicastTransportOperation> operations, TableBasedQueueFactory queueFactory)
        {
            SqlConnection sqlTransportConnection;
            SqlTransaction sqlTransportTransaction;
            Transaction ambientTransaction;

            transportTransaction.TryGet(out sqlTransportConnection);
            transportTransaction.TryGet(out sqlTransportTransaction);
            transportTransaction.TryGet(out ambientTransaction);

            if (ambientTransaction != null)
            {
                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    await Send(operations, queueFactory, connection, null).ConfigureAwait(false);
                }
            }
            else
            {
                await Send(operations, queueFactory, sqlTransportConnection, sqlTransportTransaction).ConfigureAwait(false);
            }
        }

        async Task Send(List<UnicastTransportOperation> operations, TableBasedQueueFactory queueFactory, SqlConnection connection, SqlTransaction transaction)
        {
            foreach (var operation in operations)
            {
                var address = addressTranslator.Parse(operation.Destination);
                var queue = queueFactory.Get(address.QualifiedTableName, address.Address);
                await queue.Send(operation.Message.MessageId, operation.Message.Headers, operation.Message.Body, connection, transaction).ConfigureAwait(false);
            }
        }

        static bool InReceiveOnlyTransportTransactionMode(TransportTransaction transportTransaction)
        {
            bool inReceiveMode;
            return transportTransaction.TryGet(ReceiveWithNativeTransaction.ReceiveOnlyTransactionMode, out inReceiveMode);
        }

        SqlConnectionFactory connectionFactory;
        Func<TableBasedQueueFactory> queueFactoryFactory;
        QueueAddressTranslator addressTranslator;

        class DeduplicationKey
        {
            string messageId;
            string destination;

            public DeduplicationKey(string messageId, string destination)
            {
                this.messageId = messageId;
                this.destination = destination;
            }

            bool Equals(DeduplicationKey other)
            {
                return string.Equals(messageId, other.messageId) && string.Equals(destination, other.destination);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((DeduplicationKey) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (messageId.GetHashCode()*397) ^ destination.GetHashCode();
                }
            }
        }
    }
}
namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Extensibility;
    using IsolationLevel = System.Data.IsolationLevel;

    class ReceiveWithNativeTransaction
    {
        public ReceiveWithNativeTransaction(TransactionOptions transactionOptions, FailureInfoStorage failureInfoStorage, bool transactionForReceiveOnly = false)
        {
            this.failureInfoStorage = failureInfoStorage;
            this.transactionForReceiveOnly = transactionForReceiveOnly;

            isolationLevel = IsolationLevelMapper.Map(transactionOptions.IsolationLevel);
        }

        
        public async Task ReceiveMessage(TableBasedQueue inputQueue, TableBasedQueue poisonQueue, TableBasedQueueFactory queueFactory, SqlConnection connection, CancellationTokenSource receiveCancellationTokenSource)
        {
            Message message = null;
            try
            {
                using (var transaction = connection.BeginTransaction(isolationLevel))
                {
                    var receiveResult = await inputQueue.TryReceive(connection, transaction).ConfigureAwait(false);

                    if (receiveResult.IsPoison)
                    {
                        await poisonQueue.DeadLetter(receiveResult.RawData, connection, transaction).ConfigureAwait(false);
                        transaction.Commit();
                        return;
                    }
                    if (!receiveResult.Successful)
                    {
                        receiveCancellationTokenSource.Cancel();
                        return;
                    }
                    message = receiveResult.Message;
                    if (!await TryProcess(message, PrepareTransportTransaction(connection, transaction, queueFactory)).ConfigureAwait(false))
                    {
                        transaction.Rollback();
                    }
                    else
                    {
                        transaction.Commit();
                    }
                }
                failureInfoStorage.ClearFailureInfoForMessage(message.TransportId);
            }
            catch (Exception exception)
            {
                if (message == null)
                {
                    throw;
                }
                failureInfoStorage.RecordFailureInfoForMessage(message.TransportId, exception);
            }
        }

        TransportTransaction PrepareTransportTransaction(SqlConnection connection, SqlTransaction transaction, TableBasedQueueFactory queueFactory)
        {
            var transportTransaction = new TransportTransaction();

            //those resources are meant to be used by anyone except message dispatcher e.g. persister
            transportTransaction.Set(connection);
            transportTransaction.Set(transaction);
            transportTransaction.Set(queueFactory);

            if (transactionForReceiveOnly)
            {
                //this indicates to MessageDispatcher that it should not reuse connection or transaction for sends
                transportTransaction.Set(ReceiveOnlyTransactionMode, true);
            }

            return transportTransaction;
        }

        async Task<bool> TryProcess(Message message, TransportTransaction transportTransaction)
        {
            FailureInfoStorage.ProcessingFailureInfo failure;
            if (failureInfoStorage.TryGetFailureInfoForMessage(message.TransportId, out failure))
            {
                var errorHandlingResult = await HandleError(failure.Exception, message, transportTransaction, failure.NumberOfProcessingAttempts).ConfigureAwait(false);

                if (errorHandlingResult == ErrorHandleResult.Handled)
                {
                    return true;
                }
            }

            try
            {
                return await TryProcessingMessage(message, transportTransaction).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failureInfoStorage.RecordFailureInfoForMessage(message.TransportId, exception);
                return false;
            }
        }

        async Task<bool> TryProcessingMessage(Message message, TransportTransaction transportTransaction)
        {
            using (var pushCancellationTokenSource = new CancellationTokenSource())
            {
                var messageContext = new MessageContext(message.TransportId, message.Headers, message.Body, transportTransaction, pushCancellationTokenSource, new ContextBag());

                await onMessage(messageContext).ConfigureAwait(false);

                // Cancellation is requested when message processing is aborted.
                // We return the opposite value:
                //  - true when message processing completed successfully,
                //  - false when message processing was aborted.
                return !pushCancellationTokenSource.Token.IsCancellationRequested;
            }
        }

        async Task<ErrorHandleResult> HandleError(Exception exception, Message message, TransportTransaction transportTransaction, int processingAttempts)
        {
            try
            {
                var errorContext = new ErrorContext(exception, message.Headers, message.TransportId, message.Body, transportTransaction, processingAttempts);

                return await onError(errorContext).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                criticalError.Raise($"Failed to execute reverability actions for message `{message.TransportId}`", ex);

                return ErrorHandleResult.RetryRequired;
            }
        }

        IsolationLevel isolationLevel;
        FailureInfoStorage failureInfoStorage;
        bool transactionForReceiveOnly;
        internal static string ReceiveOnlyTransactionMode = "SqlTransport.ReceiveOnlyTransactionMode";
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        CriticalError criticalError;

        public void Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError)
        {
            this.onMessage = onMessage;
            this.onError = onError;
            this.criticalError = criticalError;
        }
    }
}
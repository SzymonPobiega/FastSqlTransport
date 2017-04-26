namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Unicast.Queuing;
    using static System.String;

    class TableBasedQueue
    {
        public string Name { get; }

        public TableBasedQueue(string qualifiedTableName, string queueName, IQueueFullHandling queueFullHandling)
        {
#pragma warning disable 618
            this.qualifiedTableName = qualifiedTableName;
            this.queueFullHandling = queueFullHandling;
            Name = queueName;
            receiveCommand = Format(SqlConstants.ReceiveText, this.qualifiedTableName);
            sendCommand = Format(SqlConstants.SendText, this.qualifiedTableName);
            purgeCommand = Format(SqlConstants.PurgeText, this.qualifiedTableName);
#pragma warning restore 618
        }

        public async Task<MessageReadResult> TryReceive(SqlConnection connection, SqlTransaction transaction)
        {
            using (var command = new SqlCommand(receiveCommand, connection, transaction))
            {
                command.Parameters.AddWithValue("@seq", receivedSeq);
                using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess).ConfigureAwait(false))
                {
                    if (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var readResult = await MessageRow.Read(reader).ConfigureAwait(false);
                        receivedSeq = readResult.RawData.Seq;
                        return readResult;
                    }
                }
            }

            //No free slot, table might be empty or we are approaching end of buffer and need to start from beginning
            var nextSequence = await GetNextSequence(connection, transaction, true).ConfigureAwait(false);
            if (nextSequence.HasValue)
            {
                receivedSeq = nextSequence.Value - 1;
                var result = await TryReceive(connection, transaction).ConfigureAwait(false);
                return result;
            }
            return MessageReadResult.NoMessage;
        }

        async Task<int?> GetNextSequence(SqlConnection connection, SqlTransaction transaction, bool hasMessage)
        {
            using (var command = new SqlCommand($@"
SELECT Min(Seq)
FROM {qualifiedTableName}
WHERE HasMessage = @hasMessage", connection, transaction))
            {
                command.Parameters.AddWithValue("@hasMessage", hasMessage);
                var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
                if (DBNull.Value == result)
                {
                    return null;
                }
                return (int)result;
            }
        }

        public Task DeadLetter(MessageRow poisonMessage, SqlConnection connection, SqlTransaction transaction)
        {
            return SendRawMessage(poisonMessage, connection, transaction);
        }

        public Task Send(string messageId, Dictionary<string, string> headers, byte[] body, SqlConnection connection, SqlTransaction transaction)
        {
            var messageRow = MessageRow.From(messageId, headers, body, sentSeq);

            return SendRawMessage(messageRow, connection, transaction);
        }

        async Task SendRawMessage(MessageRow message, SqlConnection connection, SqlTransaction transaction)
        {
            try
            {
                using (var command = new SqlCommand(sendCommand, connection, transaction))
                {
                    message.PrepareSendCommand(command);

                    using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow).ConfigureAwait(false))
                    {
                        if (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            queueFullHandling.OnQueueNonFull();
                            var result = reader.GetInt32(0);
                            sentSeq = result;
                            return;
                        }
                    }
                }
                //No free slot, table might be full or we are approaching end of buffer and need to start from beginning
                var nextSequence = await GetNextSequence(connection, transaction, false).ConfigureAwait(false);
                if (nextSequence.HasValue)
                {
                    //We found a free slot, let's update and retry
                    sentSeq = nextSequence.Value - 1;
                    message.UpdateSeq(sentSeq);
                    await SendRawMessage(message, connection, transaction).ConfigureAwait(false);
                }
                else
                {
                    //No free slot, let's apply queue full handling strategy and (possibly) retry.
                    await queueFullHandling.HandleQueueFull().ConfigureAwait(false);
                    await SendRawMessage(message, connection, transaction).ConfigureAwait(false);
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == 208)
                {
                    ThrowQueueNotFoundException(ex);
                }

                ThrowFailedToSendException(ex);
            }
            catch (Exception ex)
            {
                ThrowFailedToSendException(ex);
            }
        }

        void ThrowQueueNotFoundException(SqlException ex)
        {
            throw new QueueNotFoundException(Name, $"Failed to send message to {qualifiedTableName}", ex);
        }

        void ThrowFailedToSendException(Exception ex)
        {
            throw new Exception($"Failed to send message to {qualifiedTableName}", ex);
        }

        public async Task<int> Purge(SqlConnection connection)
        {
            using (var command = new SqlCommand(purgeCommand, connection))
            {
                return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public override string ToString()
        {
            return qualifiedTableName;
        }

        int receivedSeq;
        int sentSeq;

        string qualifiedTableName;
        readonly IQueueFullHandling queueFullHandling;
        string receiveCommand;
        string sendCommand;
        string purgeCommand;
    }
}
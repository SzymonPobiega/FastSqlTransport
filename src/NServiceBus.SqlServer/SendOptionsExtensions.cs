namespace NServiceBus.Transport.SQLServer
{
    using System.Data.SqlClient;
    using Extensibility;

    /// <summary>
    /// SQL-specific extensions to send options.
    /// </summary>
    public static class SendOptionsExtensions
    {
        /// <summary>
        /// Enables fast sending mode using specified queue full handling.
        /// </summary>
        /// <param name="options">Options.</param>
        /// <param name="queueFullHandling">Strategy for dealing with full queue.</param>
        /// <param name="connection">Open SQL connection</param>
        public static void UseFastSending(this SendOptions options, IQueueFullHandling queueFullHandling, SqlConnection connection)
        {
            var transaction = new TransportTransaction();
            transaction.Set(connection);
            transaction.Set(new TableBasedQueueFactory(queueFullHandling));
            options.GetExtensions().Set(transaction);
        }
    }
}
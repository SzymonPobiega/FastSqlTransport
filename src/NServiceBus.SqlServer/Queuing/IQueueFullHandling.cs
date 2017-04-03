namespace NServiceBus.Transport.SQLServer
{
    using System.Threading.Tasks;

    /// <summary>
    /// Allows to customize how full queue should be handled when sending.
    /// </summary>
    public interface IQueueFullHandling
    {
        /// <summary>
        /// Called when send failed because destination queue is full.
        /// </summary>
        Task HandleQueueFull();

        /// <summary>
        /// Called after sending, when destination queue is not full.
        /// </summary>
        void OnQueueNonFull();
    }
}
namespace NServiceBus.Transport.SQLServer
{
    using System.Threading;
    using System.Threading.Tasks;

    interface IQueueEmptyHandling
    {
        Task HandleQueueEmpty(CancellationToken stopToken);
        void OnQueueNonEmpty();
    }
}
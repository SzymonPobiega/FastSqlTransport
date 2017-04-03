namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// A strategy that handles full queue by waiting.
    /// </summary>
    public class ProgressiveDelayQueueFullHandling : IQueueFullHandling
    {
        int attempt;
        int maxAttempts;
        int delayStepInMilliseconds;

        /// <summary>
        /// Creates new strategy instance.
        /// </summary>
        /// <param name="maxAttempts"></param>
        /// <param name="delayStepInMilliseconds"></param>
        public ProgressiveDelayQueueFullHandling(int maxAttempts, int delayStepInMilliseconds)
        {
            this.maxAttempts = maxAttempts;
            this.delayStepInMilliseconds = delayStepInMilliseconds;
        }

        /// <summary>
        /// Called when send failed because destination queue is full.
        /// </summary>
        public Task HandleQueueFull()
        {
            Console.Write("f");
            attempt++;
            if (attempt > maxAttempts)
            {
                throw new Exception("Queue is full.");
            }
            return Task.Delay(attempt * delayStepInMilliseconds);
        }

        /// <summary>
        /// Called after sending, when destination queue is not full.
        /// </summary>
        public void OnQueueNonFull()
        {
            attempt = 0;
        }
    }
}
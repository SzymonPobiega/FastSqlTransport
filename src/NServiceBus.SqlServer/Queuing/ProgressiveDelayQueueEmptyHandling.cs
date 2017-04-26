namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    class ProgressiveDelayQueueEmptyHandling : IQueueEmptyHandling
    {
        int delayStepMilliseconds;
        int maxDelayMillisecond;

        int consecutiveFailures;

        public ProgressiveDelayQueueEmptyHandling(int delayStepMilliseconds, int maxDelayMillisecond)
        {
            this.delayStepMilliseconds = delayStepMilliseconds;
            this.maxDelayMillisecond = maxDelayMillisecond;
        }

        public Task HandleQueueEmpty(CancellationToken stopToken)
        {
            consecutiveFailures++;
            Console.Write("e");
            return Task.Delay(CalculateDelay(), stopToken);
        }

        int CalculateDelay()
        {
            return Math.Min(maxDelayMillisecond, consecutiveFailures * delayStepMilliseconds);
        }

        public void OnQueueNonEmpty()
        {
            consecutiveFailures = 0;
        }
    }
}
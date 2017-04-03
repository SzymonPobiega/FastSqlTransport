namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Collections.Generic;

    class TableBasedQueueFactory
    {
        public TableBasedQueueFactory(IQueueFullHandling queueFullHandling)
        {
            this.queueFullHandling = queueFullHandling;
        }

        public TableBasedQueue Get(string qualifiedTableName, string queueName)
        {
            var key = Tuple.Create(qualifiedTableName, queueName);
            TableBasedQueue value;
            if (!cache.TryGetValue(key, out value))
            {
                value = new TableBasedQueue(qualifiedTableName, queueName, queueFullHandling);
                cache[key] = value;
            }
            return value;
        } 

        Dictionary<Tuple<string, string>, TableBasedQueue> cache = new Dictionary<Tuple<string, string>, TableBasedQueue>();
        IQueueFullHandling queueFullHandling;
    }
}
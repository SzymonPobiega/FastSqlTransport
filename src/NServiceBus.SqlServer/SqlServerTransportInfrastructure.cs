namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using System.Transactions;
    using Performance.TimeToBeReceived;
    using Routing;
    using Settings;
    using Transport;

    class SqlServerTransportInfrastructure : TransportInfrastructure
    {
        internal SqlServerTransportInfrastructure(QueueAddressTranslator addressTranslator, SettingsHolder settings, string connectionString)
        {
            this.addressTranslator = addressTranslator;
            this.settings = settings;
            this.connectionString = connectionString;

            this.schemaAndCatalogSettings = settings.GetOrCreate<EndpointSchemaAndCatalogSettings>();

            //HINT: this flag indicates that user need to explicitly turn outbox in configuration.
        }

        public override IEnumerable<Type> DeliveryConstraints { get; } = new[]
        {
            typeof(DiscardIfNotReceivedBefore)
        };

        public override TransportTransactionMode TransactionMode { get; } = TransportTransactionMode.TransactionScope;

        public override OutboundRoutingPolicy OutboundRoutingPolicy { get; } = new OutboundRoutingPolicy(OutboundRoutingType.Unicast, OutboundRoutingType.Unicast, OutboundRoutingType.Unicast);

        public override TransportReceiveInfrastructure ConfigureReceiveInfrastructure()
        {
            SqlScopeOptions scopeOptions;
            if (!settings.TryGet(out scopeOptions))
            {
                scopeOptions = new SqlScopeOptions();
            }

            TimeSpan waitTimeCircuitBreaker;
            if (!settings.TryGet(SettingsKeys.TimeToWaitBeforeTriggering, out waitTimeCircuitBreaker))
            {
                waitTimeCircuitBreaker = TimeSpan.FromSeconds(30);
            }

            var connectionFactory = CreateConnectionFactory();

            Func<TransportTransactionMode, ReceiveWithNativeTransaction> receiveStrategyFactory =
                guarantee => SelectReceiveStrategy(guarantee, scopeOptions.TransactionOptions);

            var queuePurger = new QueuePurger(connectionFactory);

            return new TransportReceiveInfrastructure(
                () => new MessagePump(receiveStrategyFactory, GetQueueFactoryFactory(), queuePurger, connectionFactory, () => new ProgressiveDelayQueueEmptyHandling(10, 100), addressTranslator, waitTimeCircuitBreaker),
                () => new QueueCreator(connectionFactory, addressTranslator),
                () => Task.FromResult(StartupCheckResult.Success));
        }

        SqlConnectionFactory CreateConnectionFactory()
        {
            Func<Task<SqlConnection>> factoryOverride;

            if (settings.TryGet(SettingsKeys.ConnectionFactoryOverride, out factoryOverride))
            {
                return new SqlConnectionFactory(factoryOverride);
            }

            return SqlConnectionFactory.Default(connectionString);
        }

        static ReceiveWithNativeTransaction SelectReceiveStrategy(TransportTransactionMode minimumConsistencyGuarantee, TransactionOptions options)
        {
            if (minimumConsistencyGuarantee == TransportTransactionMode.TransactionScope)
            {
                throw new Exception($"Transaction mode {minimumConsistencyGuarantee} is not supported");
            }

            if (minimumConsistencyGuarantee == TransportTransactionMode.SendsAtomicWithReceive)
            {
                return new ReceiveWithNativeTransaction(options, new FailureInfoStorage(10000));
            }
            return new ReceiveWithNativeTransaction(options, new FailureInfoStorage(10000), transactionForReceiveOnly: true);
        }

        public override TransportSendInfrastructure ConfigureSendInfrastructure()
        {
            var connectionFactory = CreateConnectionFactory();
            settings.Get<EndpointInstances>().AddOrReplaceInstances("SqlServer", schemaAndCatalogSettings.ToEndpointInstances());
            return new TransportSendInfrastructure(
                () => new MessageDispatcher(addressTranslator, connectionFactory, GetQueueFactoryFactory()),
                () => Task.FromResult(StartupCheckResult.Success)
                );
        }

        static Func<TableBasedQueueFactory> GetQueueFactoryFactory()
        {
            return () => new TableBasedQueueFactory(GetQueueFullHandling());
        }

        static ProgressiveDelayQueueFullHandling GetQueueFullHandling()
        {
            return new ProgressiveDelayQueueFullHandling(5, 50);
        }

        public override TransportSubscriptionInfrastructure ConfigureSubscriptionInfrastructure()
        {
            throw new NotImplementedException();
        }

        public override EndpointInstance BindToLocalEndpoint(EndpointInstance instance)
        {
            var schemaSettings = settings.Get<EndpointSchemaAndCatalogSettings>();

            string schema;
            if (schemaSettings.TryGet(instance.Endpoint, out schema) == false)
            {
                schema = addressTranslator.DefaultSchema;
            }
            var result = instance.SetProperty(SettingsKeys.SchemaPropertyKey, schema);
            if (addressTranslator.DefaultCatalog != null)
            {
                result = result.SetProperty(SettingsKeys.CatalogPropertyKey, addressTranslator.DefaultCatalog);
            }
            return result;
        }

        public override string ToTransportAddress(LogicalAddress logicalAddress)
        {
            return addressTranslator.Generate(logicalAddress).Value;
        }

        public override string MakeCanonicalForm(string transportAddress)
        {
            return addressTranslator.Parse(transportAddress).Address;
        }

        QueueAddressTranslator addressTranslator;
        string connectionString;
        SettingsHolder settings;
        EndpointSchemaAndCatalogSettings schemaAndCatalogSettings;
    }
}
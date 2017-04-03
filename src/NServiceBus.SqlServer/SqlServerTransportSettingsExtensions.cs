namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Configuration.AdvanceExtensibility;

    /// <summary>
    /// Adds extra configuration for the Sql Server transport.
    /// </summary>
    public static partial class SqlServerTransportSettingsExtensions
    {
        /// <summary>
        /// Sets a default schema for both input and output queues
        /// </summary>
        public static TransportExtensions<SqlServerTransport> DefaultSchema(this TransportExtensions<SqlServerTransport> transportExtensions, string schemaName)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNullAndEmpty(nameof(schemaName), schemaName);

            transportExtensions.GetSettings().Set(SettingsKeys.DefaultSchemaSettingsKey, schemaName);

            return transportExtensions;
        }

        /// <summary>
        /// Specifies custom schema for given endpoint.
        /// </summary>
        /// <param name="transportExtensions">The <see cref="TransportExtensions{T}" /> to extend.</param>
        /// <param name="endpointName">Endpoint name.</param>
        /// <param name="schema">Custom schema value.</param>
        /// <returns></returns>
        public static TransportExtensions<SqlServerTransport> UseSchemaForEndpoint(this TransportExtensions<SqlServerTransport> transportExtensions, string endpointName, string schema)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNullAndEmpty(nameof(endpointName), endpointName);
            Guard.AgainstNullAndEmpty(nameof(schema), schema);

            var schemasConfiguration = transportExtensions.GetSettings().GetOrCreate<EndpointSchemaAndCatalogSettings>();

            schemasConfiguration.SpecifySchema(endpointName, schema);

            return transportExtensions;
        }

        /// <summary>
        /// Overrides schema value for given queue. This setting will take precedence over any other source of schema information.
        /// </summary>
        /// <param name="transportExtensions">The <see cref="TransportExtensions{T}" /> to extend.</param>
        /// <param name="queueName">Queue name.</param>
        /// <param name="schema">Custom schema value.</param>
        /// <returns></returns>
        public static TransportExtensions<SqlServerTransport> UseSchemaForQueue(this TransportExtensions<SqlServerTransport> transportExtensions, string queueName, string schema)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNullAndEmpty(nameof(queueName), queueName);
            Guard.AgainstNullAndEmpty(nameof(schema), schema);

            var schemasConfiguration = transportExtensions.GetSettings().GetOrCreate<QueueSchemaAndCatalogSettings>();

            schemasConfiguration.SpecifySchema(queueName, schema);

            return transportExtensions;
        }

        /// <summary>
        /// Specifies custom schema for given endpoint.
        /// </summary>
        /// <param name="transportExtensions">The <see cref="TransportExtensions{T}" /> to extend.</param>
        /// <param name="endpointName">Endpoint name.</param>
        /// <param name="catalog">Custom catalog value.</param>
        /// <returns></returns>
        public static TransportExtensions<SqlServerTransport> UseCatalogForEndpoint(this TransportExtensions<SqlServerTransport> transportExtensions, string endpointName, string catalog)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNullAndEmpty(nameof(endpointName), endpointName);
            Guard.AgainstNullAndEmpty(nameof(catalog), catalog);

            var settings = transportExtensions.GetSettings();

            settings.Set(SettingsKeys.MultiCatalogEnabled, true);
            var schemasConfiguration = settings.GetOrCreate<EndpointSchemaAndCatalogSettings>();

            schemasConfiguration.SpecifyCatalog(endpointName, catalog);

            return transportExtensions;
        }

        /// <summary>
        /// Specifies custom schema for given queue.
        /// </summary>
        /// <param name="transportExtensions">The <see cref="TransportExtensions{T}" /> to extend.</param>
        /// <param name="queueName">Queue name.</param>
        /// <param name="catalog">Custom catalog value.</param>
        /// <returns></returns>
        public static TransportExtensions<SqlServerTransport> UseCatalogForQueue(this TransportExtensions<SqlServerTransport> transportExtensions, string queueName, string catalog)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNullAndEmpty(nameof(queueName), queueName);
            Guard.AgainstNullAndEmpty(nameof(catalog), catalog);

            var settings = transportExtensions.GetSettings();

            settings.Set(SettingsKeys.MultiCatalogEnabled, true);
            var schemasConfiguration = settings.GetOrCreate<QueueSchemaAndCatalogSettings>();

            schemasConfiguration.SpecifyCatalog(queueName, catalog);

            return transportExtensions;
        }

        /// <summary>
        /// Overrides the default time to wait before triggering a circuit breaker that initiates the endpoint shutdown procedure
        /// in case there are numerous errors
        /// while trying to receive messages.
        /// </summary>
        /// <param name="transportExtensions">The <see cref="TransportExtensions{T}" /> to extend.</param>
        /// <param name="waitTime">Time to wait before triggering the circuit breaker.</param>
        public static TransportExtensions<SqlServerTransport> TimeToWaitBeforeTriggeringCircuitBreaker(this TransportExtensions<SqlServerTransport> transportExtensions, TimeSpan waitTime)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNegativeAndZero(nameof(waitTime), waitTime);

            transportExtensions.GetSettings().Set(SettingsKeys.TimeToWaitBeforeTriggering, waitTime);
            return transportExtensions;
        }

        /// <summary>
        /// Specifies connection factory to be used by sql transport.
        /// </summary>
        /// <param name="transportExtensions">The <see cref="TransportExtensions{T}" /> to extend.</param>
        /// <param name="sqlConnectionFactory">Factory that returns connection ready for usage.</param>
        public static TransportExtensions<SqlServerTransport> UseCustomSqlConnectionFactory(this TransportExtensions<SqlServerTransport> transportExtensions, Func<Task<SqlConnection>> sqlConnectionFactory)
        {
            Guard.AgainstNull(nameof(transportExtensions), transportExtensions);
            Guard.AgainstNull(nameof(sqlConnectionFactory), sqlConnectionFactory);

            transportExtensions.GetSettings().Set(SettingsKeys.ConnectionFactoryOverride, sqlConnectionFactory);

            return transportExtensions;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Routing;
using Janitor;
using NServiceBus.Settings;
using NServiceBus.Kafka.Sending;
using NServiceBus.Performance.TimeToBeReceived;
using NServiceBus.Transport.Kafka.Receiving;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus.Serialization;
using NServiceBus.Transports.Kafka.Administration;

namespace NServiceBus.Transport.Kafka
{

    [SkipWeaving]
    class KafkaTransportInfrastructure : TransportInfrastructure, IDisposable
    {
        readonly SettingsHolder settings;
        readonly string connectionString;
        private static MessageWrapperSerializer serializer;
        static Object o = new Object();

        public KafkaTransportInfrastructure(SettingsHolder settings, string connectionString)
        {
            this.settings = settings;
            this.connectionString = connectionString;
            serializer = BuildSerializer(settings);

        }

        public override TransportReceiveInfrastructure ConfigureReceiveInfrastructure()
        {
            return new TransportReceiveInfrastructure(() => new MessagePump(new Transports.Kafka.Connection.ConsumerFactory(connectionString, settings.EndpointName()), settings.EndpointName()), 
                () => new QueueCreator(), 
                () => Task.FromResult(StartupCheckResult.Success));
        }

        public override TransportSendInfrastructure ConfigureSendInfrastructure()
        {
            return new TransportSendInfrastructure(() => new MessageDispatcher(new Transports.Kafka.Connection.ProducerFactory(connectionString)), () => Task.FromResult(StartupCheckResult.Success));
        }


        public override IEnumerable<Type> DeliveryConstraints
        {
            get
            {
                yield return typeof(DiscardIfNotReceivedBefore);
            }
        }

        public override OutboundRoutingPolicy OutboundRoutingPolicy
        {
            get
            {
                return new OutboundRoutingPolicy(
                    sends: OutboundRoutingType.Unicast,
                    publishes: OutboundRoutingType.Multicast,
                    replies: OutboundRoutingType.Unicast);
            }
        }

        public override TransportTransactionMode TransactionMode
        {
            get
            {
                return TransportTransactionMode.ReceiveOnly;
            }
        }

        public override EndpointInstance BindToLocalEndpoint(EndpointInstance instance)
        {
            return instance;
        }

      
        public override TransportSubscriptionInfrastructure ConfigureSubscriptionInfrastructure()
        {
            throw new NotImplementedException();
        }

        public override string ToTransportAddress(LogicalAddress logicalAddress)
        {
            throw new NotImplementedException();
        }

        static MessageWrapperSerializer BuildSerializer(ReadOnlySettings settings)
        {
            if (serializer == null)
            {
                lock (o)
                {
                    if (serializer == null)
                    {
                        SerializationDefinition wrapperSerializer;
                        if (settings!=null && settings.TryGet(WellKnownConfigurationKeys.MessageWrapperSerializationDefinition, out wrapperSerializer))
                        {
                            serializer = new MessageWrapperSerializer(wrapperSerializer.Configure(settings)(MessageWrapperSerializer.GetMapper()));
                        }

                        serializer = new MessageWrapperSerializer(KafkaTransport.GetMainSerializer(MessageWrapperSerializer.GetMapper(), settings));
                    }
                }
            }


            return serializer;
        }

        internal static MessageWrapperSerializer GetSerializer()
        {
            return serializer;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
       

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

             

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}

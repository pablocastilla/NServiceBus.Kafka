using NServiceBus.Extensibility;
using NServiceBus.Kafka.Sending;
using NServiceBus.Settings;
using NServiceBus.Transport;
using NServiceBus.Transport.Kafka;
using NServiceBus.Transport.Kafka.Receiving;
using NServiceBus.Transports.Kafka.Administration;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus.Unicast.Messages;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transport.Kafka.Tests
{
    class KafkaContext
    {
        public virtual int MaximumConcurrency => 1;
        protected string ENDPOINTNAME = "testendpoint6";
        protected const string ErrorQueue = "error";

        
        public void SetUp()
        {
           
            receivedMessages = new BlockingCollection<IncomingMessage>();

            var connectionString = "127.0.0.1:9092";// Environment.GetEnvironmentVariable("KafkaTransport.ConnectionString");
            SettingsHolder settingsHolder = new SettingsHolder();
           
           

            var kafkaTransport = new KafkaTransport();
            var infra = kafkaTransport.Initialize(settingsHolder, connectionString);

            messageDispatcher = new MessageDispatcher(new Transports.Kafka.Connection.ProducerFactory(connectionString));

            var consumerFactory = new Transports.Kafka.Connection.ConsumerFactory(connectionString, ENDPOINTNAME);
            messagePump = new MessagePump(consumerFactory, ENDPOINTNAME);
           

            subscriptionManager = new SubscriptionManager(consumerFactory);
            subscriptionManager.Subscribe(typeof(MyEvent), new ContextBag()); 
            subscriptionManager.Subscribe(typeof(EventBase), new ContextBag()); 
            subscriptionManager.Subscribe(typeof(SubEvent1), new ContextBag());
            subscriptionManager.Subscribe(typeof(SubEvent2), new ContextBag());
            subscriptionManager.Subscribe(typeof(MyEvent1), new ContextBag());
            subscriptionManager.Subscribe(typeof(IEvent), new ContextBag());
            subscriptionManager.Subscribe(typeof(CombinedClassAndInterface), new ContextBag());
            subscriptionManager.Subscribe(typeof(IMyEvent), new ContextBag());
            subscriptionManager.Subscribe(typeof(object), new ContextBag());

            messagePump.Init(messageContext =>
            {
                receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                return Task.FromResult(0);
            },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => Task.FromResult(0)),
                new PushSettings(ENDPOINTNAME, ErrorQueue, true, TransportTransactionMode.ReceiveOnly)
            ).GetAwaiter().GetResult();

            messagePump.Start(new PushRuntimeSettings(MaximumConcurrency));
        }

       
        public void TearDown()
        {           
            messagePump?.Stop().GetAwaiter().GetResult();

            messagePump = null;
        }

        protected bool TryWaitForMessageReceipt()
        {
            IncomingMessage message;
            return TryReceiveMessage(out message, incomingMessageTimeout);
        }

        protected Task<IncomingMessage> ReceiveMessage()
        {
            IncomingMessage message;
            if (!TryReceiveMessage(out message, incomingMessageTimeout))
            {
                throw new TimeoutException($"The message did not arrive within {incomingMessageTimeout.TotalSeconds} seconds.");
            }

            return Task.FromResult(message);
        }

        bool TryReceiveMessage(out IncomingMessage message, TimeSpan timeout) =>
            receivedMessages.TryTake(out message, timeout);

        protected virtual IEnumerable<string> AdditionalReceiverQueues => Enumerable.Empty<string>();

      
        protected MessageDispatcher messageDispatcher;
        protected SubscriptionManager subscriptionManager;
        protected MessagePump messagePump;
             
        BlockingCollection<IncomingMessage> receivedMessages;
       
        static readonly TimeSpan incomingMessageTimeout = TimeSpan.FromSeconds(120);
    }
}

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
        string ENDPOINTNAME = "testreceiver";

        [SetUp]
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

            messagePump.Init(messageContext =>
            {
                receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                return Task.FromResult(0);
            },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => Task.FromResult(0)),
                new PushSettings(ReceiverQueue, ErrorQueue, true, TransportTransactionMode.ReceiveOnly)
            ).GetAwaiter().GetResult();

            messagePump.Start(new PushRuntimeSettings(MaximumConcurrency));
        }

        [TearDown]
        public void TearDown()
        {
            messagePump?.Stop().GetAwaiter().GetResult();

           
        }

        protected bool TryWaitForMessageReceipt()
        {
            IncomingMessage message;
            return TryReceiveMessage(out message, incomingMessageTimeout);
        }

        protected IncomingMessage ReceiveMessage()
        {
            IncomingMessage message;
            if (!TryReceiveMessage(out message, incomingMessageTimeout))
            {
                throw new TimeoutException($"The message did not arrive within {incomingMessageTimeout.TotalSeconds} seconds.");
            }

            return message;
        }

        bool TryReceiveMessage(out IncomingMessage message, TimeSpan timeout) =>
            receivedMessages.TryTake(out message, timeout);

        protected virtual IEnumerable<string> AdditionalReceiverQueues => Enumerable.Empty<string>();

        protected const string ReceiverQueue = "testreceiver";
        protected const string ErrorQueue = "error";
        protected MessageDispatcher messageDispatcher;
        protected SubscriptionManager subscriptionManager;
        protected MessagePump messagePump;
             
        BlockingCollection<IncomingMessage> receivedMessages;
       
        static readonly TimeSpan incomingMessageTimeout = TimeSpan.FromSeconds(20);
    }
}

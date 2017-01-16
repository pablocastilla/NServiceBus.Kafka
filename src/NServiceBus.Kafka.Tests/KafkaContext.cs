using NServiceBus.Extensibility;
using NServiceBus.Kafka.Sending;
using NServiceBus.Settings;
using NServiceBus.Transport;
using NServiceBus.Transport.Kafka;
using NServiceBus.Transport.Kafka.Receiving;
using NServiceBus.Transports.Kafka.Administration;
using NServiceBus.Transports.Kafka.Connection;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus.Unicast.Messages;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.Kafka.Tests
{
    class KafkaContext
    {
        public virtual int MaximumConcurrency => 1;
        protected string endpointName = "endpointfortests";
        protected const string ErrorQueue = "error";
        
        public void SetUp (params Type[] typesToSubscribeTo)
        {
           
            receivedMessages = new BlockingCollection<IncomingMessage>();

            Environment.SetEnvironmentVariable("KafkaTransport.ConnectionString", "127.0.0.1:9092");
            var connectionString =  Environment.GetEnvironmentVariable("KafkaTransport.ConnectionString");//"127.0.0.1:9092"
            SettingsHolder settingsHolder = new SettingsHolder();

            settingsHolder.Set("NServiceBus.Routing.EndpointName", endpointName);

             var kafkaTransport = new KafkaTransport();
            var infra = kafkaTransport.Initialize(settingsHolder, connectionString);

            messageDispatcher = new MessageDispatcher(new Transports.Kafka.Connection.ProducerFactory(connectionString));

            var consumerFactory = new Transports.Kafka.Connection.ConsumerFactory(connectionString, endpointName, settingsHolder);
            messagePump = new MessagePump(endpointName, settingsHolder, connectionString);
           

            subscriptionManager = new SubscriptionManager(consumerFactory);

            foreach (var t in typesToSubscribeTo)
            {
                subscriptionManager.Subscribe(t, new ContextBag());
            }
           
           
            messagePump.Init(messageContext =>
            {
                receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                return Task.FromResult(0);
            },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => Task.FromResult(0)),
                new PushSettings(endpointName, ErrorQueue, true, TransportTransactionMode.ReceiveOnly)
            ).GetAwaiter().GetResult();

            messagePump.Start(new PushRuntimeSettings(MaximumConcurrency));

           
        }

        [TearDown]
        public void TearDown()
        {           
            //messagePump?.Stop().GetAwaiter().GetResult();

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

        protected List<IncomingMessage> ReceiveMessages(int timeoutInSeconds, int maxNumberOfMessages)
        {
            var result = new List<IncomingMessage>();

            var finalTimeout = timeoutInSeconds * 1000;
            try
            {
                int i = 0;
                while (i < maxNumberOfMessages)
                {

                    try
                    {
                        IncomingMessage message;
                        if (receivedMessages.TryTake(out message, finalTimeout))
                        {
                            result.Add(message);
                            finalTimeout = 1000;
                        }
                                              
                    }
                    catch(Exception ex)
                    {

                    }

                    i++;
                }
            }
            catch (Exception)
            {

            }

            return result;

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

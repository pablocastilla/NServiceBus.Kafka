using NServiceBus.Kafka.Receiving;
using NServiceBus.Kafka.Sending;
using NServiceBus.Transport;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Kafka.Tests
{
    class KafkaContext
    {
        public virtual int MaximumConcurrency => 1;

        [SetUp]
        public void SetUp()
        {
           
            receivedMessages = new BlockingCollection<IncomingMessage>();

            var connectionString = Environment.GetEnvironmentVariable("KafkaTransport.ConnectionString");

           
            messageDispatcher = new MessageDispatcher();


            messagePump = new MessagePump( new MessageConverter(), "Unit test", channelProvider, TimeSpan.FromMinutes(2), 3, 0);

            routingTopology.Reset(connectionFactory, new[] { ReceiverQueue }.Concat(AdditionalReceiverQueues), new[] { ErrorQueue });

            subscriptionManager = new SubscriptionManager(connectionFactory, routingTopology, ReceiverQueue);

            messagePump.Init(messageContext =>
            {
                receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                return TaskEx.CompletedTask;
            },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => TaskEx.CompletedTask),
                new PushSettings(ReceiverQueue, ErrorQueue, true, TransportTransactionMode.ReceiveOnly)
            ).GetAwaiter().GetResult();

            messagePump.Start(new PushRuntimeSettings(MaximumConcurrency));
        }

        [TearDown]
        public void TearDown()
        {
            messagePump?.Stop().GetAwaiter().GetResult();

            channelProvider?.Dispose();
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
        
        protected MessagePump messagePump;
             
        BlockingCollection<IncomingMessage> receivedMessages;
       
        static readonly TimeSpan incomingMessageTimeout = TimeSpan.FromSeconds(1);
    }
}

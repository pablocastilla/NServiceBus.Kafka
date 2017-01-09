using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Extensibility;
using NServiceBus.Transports.Kafka.Connection;
using NServiceBus.Transports.Kafka.Wrapper;
using NServiceBus.Performance.TimeToBeReceived;
using NServiceBus.DeliveryConstraints;
using System.IO;
using NServiceBus.Transport.Kafka;
using NServiceBus.Transports.Kafka.Administration;
using NServiceBus.Logging;

namespace NServiceBus.Kafka.Sending
{
    class MessageDispatcher : IDispatchMessages
    {
        private ProducerFactory producerFactory;
        static ILog Logger = LogManager.GetLogger(typeof(MessageDispatcher));

        public MessageDispatcher(ProducerFactory producerFactory)
        {
            this.producerFactory = producerFactory;
        }

        public async Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, ContextBag context)
        {
            Logger.Info("Dispatch");

            try
            {
                var unicastTransportOperations = outgoingMessages.UnicastTransportOperations;
                var multicastTransportOperations = outgoingMessages.MulticastTransportOperations;

                var tasks = new List<Task>(unicastTransportOperations.Count + multicastTransportOperations.Count);

                foreach (var operation in unicastTransportOperations)
                {
                    tasks.Add(SendMessage(operation));
                }

                foreach (var operation in multicastTransportOperations)
                {                    
                    tasks.Add(PublishMessage(operation));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            finally
            {
                
            }
        }

        async Task SendMessage(UnicastTransportOperation transportOperation)
        {           
            var messageWrapper = BuildMessageWrapper(transportOperation, TimeSpan.MaxValue, transportOperation.Destination);

            var topic = producerFactory.GetProducer().Topic(transportOperation.Destination);

            var messageStream = new MemoryStream();
            KafkaTransportInfrastructure.GetSerializer().Serialize(messageWrapper, messageStream);            
           
            await topic.Produce(messageStream.ToArray());
        }

        async Task PublishMessage(MulticastTransportOperation transportOperation)
        {
            var messageWrapper = BuildMessageWrapper(transportOperation, TimeSpan.MaxValue, transportOperation.MessageType.ToString());

            var topicsToSendTo = SubscriptionManager.GetTypeHierarchy(transportOperation.MessageType);

            var messageStream = new MemoryStream();
            KafkaTransportInfrastructure.GetSerializer().Serialize(messageWrapper, messageStream);

            foreach (var t in topicsToSendTo)
            {
                var topic = producerFactory.GetProducer().Topic(t);                              

                await topic.Produce(messageStream.ToArray()).ContinueWith(result => Logger.Info("new partition and offset: "+result.Result.Partition+" "+result.Result.Offset));
            }

           
        }

        MessageWrapper BuildMessageWrapper(IOutgoingTransportOperation operation, TimeSpan? timeToBeReceived, string destinationQueue)
        {
            var msg = operation.Message;
            var headers = new Dictionary<string, string>(msg.Headers);
          
            var messageIntent = default(MessageIntentEnum);
            string messageIntentString;
            if (headers.TryGetValue(Headers.MessageIntent, out messageIntentString))
            {
                Enum.TryParse(messageIntentString, true, out messageIntent);
            }

            return new MessageWrapper
            {
                Id = msg.MessageId,
                Body = msg.Body,
                CorrelationId = headers.GetValueOrDefault(Headers.CorrelationId),
                Recoverable = operation.GetDeliveryConstraint<NonDurableDelivery>() == null,
                ReplyToAddress = headers.GetValueOrDefault(Headers.ReplyToAddress),
                TimeToBeReceived = timeToBeReceived ?? TimeSpan.MaxValue,
                Headers = headers,
                MessageIntent = messageIntent
            };
        }
    }

    static class DictionaryExtensions
    {
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue defaultValue = default(TValue))
        {
            TValue value;
            return dictionary.TryGetValue(key, out value) ? value : defaultValue;
        }
    }

    static class TransportOperationExtensions
    {
        public static TimeSpan? GetTimeToBeReceived(this UnicastTransportOperation operation)
        {
            return operation.GetDeliveryConstraint<DiscardIfNotReceivedBefore>()?.MaxTime;
        }

        public static T GetDeliveryConstraint<T>(this IOutgoingTransportOperation operation)
            where T : DeliveryConstraint
        {
            return operation.DeliveryConstraints.OfType<T>().FirstOrDefault();
        }
    }
}

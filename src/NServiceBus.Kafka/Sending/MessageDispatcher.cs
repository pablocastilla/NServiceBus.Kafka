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

namespace NServiceBus.Kafka.Sending
{
    class MessageDispatcher : IDispatchMessages
    {
        private ProducerFactory producerFactory;

        public MessageDispatcher(ProducerFactory producerFactory)
        {
            this.producerFactory = producerFactory;
        }

        public Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, ContextBag context)
        {
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

                return tasks.Count == 1 ? tasks[0] : Task.WhenAll(tasks);
            }
            finally
            {
                
            }
        }

        Task SendMessage(UnicastTransportOperation transportOperation)
        {           
            var messageWrapper = BuildMessageWrapper(transportOperation, TimeSpan.MaxValue, transportOperation.Destination);

            var topic = producerFactory.GetProducer().Topic(transportOperation.Destination);

            var messageStream = new MemoryStream();
            KafkaTransportInfrastructure.GetSerializer().Serialize(messageWrapper, messageStream);
            
            return topic.Produce(messageStream.ToArray());
        }

        Task PublishMessage(MulticastTransportOperation transportOperation)
        {
            var messageWrapper = BuildMessageWrapper(transportOperation, TimeSpan.MaxValue, transportOperation.MessageType.ToString());

            var topic = producerFactory.GetProducer().Topic(transportOperation.MessageType.ToString());

            var messageStream = new MemoryStream();
            KafkaTransportInfrastructure.GetSerializer().Serialize(messageWrapper, messageStream);

            return topic.Produce(messageStream.ToArray());
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

using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
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

        public Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction,
            ContextBag context)
        {
            var unicastTransportOperations = outgoingMessages.UnicastTransportOperations;
            var multicastTransportOperations = outgoingMessages.MulticastTransportOperations;

            var tasks = new List<Task>(unicastTransportOperations.Count + multicastTransportOperations.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var operation in unicastTransportOperations)
            {
                tasks.Add(SendMessage(operation));
            }

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var operation in multicastTransportOperations)
            {
                tasks.Add(PublishMessage(operation));
            }

            return Task.WhenAll(tasks);
        }

        Task SendMessage(UnicastTransportOperation transportOperation)
        {
            Logger.Debug("Send to " + transportOperation.Destination);

            var toBeReceived = transportOperation.GetTimeToBeReceived();
            var timeToBeReceived = toBeReceived.HasValue && toBeReceived.Value < TimeSpan.MaxValue
                ? toBeReceived
                : null;

            if (timeToBeReceived != null && timeToBeReceived.Value == TimeSpan.Zero)
            {
                var messageType = transportOperation.Message.Headers[Headers.EnclosedMessageTypes].Split(',').First();
                Logger.WarnFormat("TimeToBeReceived is set to zero for message of type '{0}'. Cannot send operation.",
                    messageType);
                return Task.FromResult(0);
            }

            // TimeToBeReceived was not specified on message - go for maximum set by SDK
            if (timeToBeReceived == TimeSpan.MaxValue)
            {
                timeToBeReceived = null;
            }

            var messageWrapper = BuildMessageWrapper(transportOperation, toBeReceived, transportOperation.Destination);

            var topic = producerFactory.GetProducer().Topic(transportOperation.Destination);

            var messageStream = new MemoryStream();
            KafkaTransportInfrastructure.GetSerializer().Serialize(messageWrapper, messageStream);

            return topic.Produce(messageStream.ToArray());
        }

        Task PublishMessage(MulticastTransportOperation transportOperation)
        {
            var messageWrapper = BuildMessageWrapper(transportOperation, TimeSpan.MaxValue,
                transportOperation.MessageType.ToString());

            var topicsToSendTo = SubscriptionManager.GetTypeHierarchy(transportOperation.MessageType);

            var messageStream = new MemoryStream();
            KafkaTransportInfrastructure.GetSerializer().Serialize(messageWrapper, messageStream);

            var publishTasks = new List<Task>(topicsToSendTo.Count);

            foreach (var t in topicsToSendTo)
            {
                Logger.Debug("Publish to " + t);
                var topic = producerFactory.GetProducer().Topic(t);

                publishTasks.Add(topic.Produce(messageStream.ToArray())
                    .ContinueWith(
                        result => Logger.Info("new partition and offset: " + result.Result.Partition + " " +
                                              result.Result.Offset), TaskContinuationOptions.ExecuteSynchronously));
            }

            return Task.WhenAll(publishTasks);
        }

        static MessageWrapper BuildMessageWrapper(IOutgoingTransportOperation operation, TimeSpan? timeToBeReceived,
            string destinationQueue)
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
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key,
            TValue defaultValue = default(TValue))
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
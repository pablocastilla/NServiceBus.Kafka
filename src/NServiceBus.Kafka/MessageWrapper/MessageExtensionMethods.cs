using NServiceBus.Transport.Kafka;
using RdKafka;
using System;
using System.IO;

namespace NServiceBus.Transports.Kafka.Wrapper
{
    internal static class MessageExtensionMethods
    {
        internal static MessageWrapper UnWrap(this Message kafkaMessage)
        {
            MessageWrapper m;
            using (var stream = new MemoryStream(kafkaMessage.Payload))
            {
                m = KafkaTransportInfrastructure.GetSerializer().Deserialize(stream);
            }

            if (m == null)
            {
                throw new ArgumentNullException(nameof(kafkaMessage));
            }

            if (m.ReplyToAddress != null)
            {
                m.Headers[Headers.ReplyToAddress] = m.ReplyToAddress;
            }
            m.Headers[Headers.CorrelationId] = m.CorrelationId;

            if (m.TimeToBeReceived != TimeSpan.MaxValue)
            {
                m.Headers[Headers.TimeToBeReceived] = m.TimeToBeReceived.ToString();
            }
            m.Headers[Headers.MessageIntent] = m.MessageIntent.ToString(); // message intent extension method

            return m;
        }
    }
}
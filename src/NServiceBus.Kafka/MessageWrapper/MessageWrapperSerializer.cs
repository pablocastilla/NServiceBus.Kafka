namespace NServiceBus.Transports.Kafka.Wrapper
{
    using System;
    using System.IO;
    using System.Linq;
    using MessageInterfaces.MessageMapper.Reflection;
    using Serialization;

    class MessageWrapperSerializer
    {
        public MessageWrapperSerializer(IMessageSerializer messageSerializer)
        {
            this.messageSerializer = messageSerializer;
        }

        public static MessageMapper GetMapper()
        {
            var mapper = new MessageMapper();
            mapper.Initialize(MessageTypes);
            return mapper;
        }

        public void Serialize(MessageWrapper wrapper, Stream stream)
        {
            messageSerializer.Serialize(wrapper, stream);
        }

        public MessageWrapper Deserialize(Stream stream)
        {
            return messageSerializer.Deserialize(stream, MessageTypes).SingleOrDefault() as MessageWrapper;
        }

        readonly IMessageSerializer messageSerializer;

        static readonly Type[] MessageTypes =
        {
            typeof(MessageWrapper)
        };
    }
}
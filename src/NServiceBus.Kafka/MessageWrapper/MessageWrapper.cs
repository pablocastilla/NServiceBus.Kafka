namespace NServiceBus.Transports.Kafka.Wrapper
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class MessageWrapper : IMessage
    {
        public string IdForCorrelation { get; set; }

        public string Id { get; set; }

        public MessageIntentEnum MessageIntent { get; set; }

        public string ReplyToAddress { get; set; }

        public TimeSpan TimeToBeReceived { get; set; }

        public Dictionary<string, string> Headers { get; set; }

        public byte[] Body { get; set; }

        public string CorrelationId { get; set; }

        public bool Recoverable { get; set; }
    }
}
using NServiceBus.Extensibility;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Transport.Kafka.Tests
{
    [TestFixture]
    class When_consuming_messages:KafkaContext
    {
        [Test]
        public async Task Should_block_until_a_message_is_available()
        {
            var message = new OutgoingMessage(Guid.NewGuid().ToString(), new Dictionary<string, string>(), new byte[0]);
            var transportOperations = new TransportOperations(new TransportOperation(message, new UnicastAddressTag(ReceiverQueue)));

            await messageDispatcher.Dispatch(transportOperations, new TransportTransaction(), new ContextBag());

            var receivedMessage = ReceiveMessage();

            Assert.AreEqual(message.MessageId, receivedMessage.MessageId);


        }
    }
}

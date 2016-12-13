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
   /* [TestFixture]
    class When_consuming_messages:KafkaContext
    {
        [OneTimeSetUp]
        public void SetUpp()
        {
            base.SetUp();
        }

        [Test]
        public async Task Should_block_until_a_message_is_available()
        {
            var message = new OutgoingMessage("fixed token", new Dictionary<string, string>(), new byte[0]);
            var transportOperations = new TransportOperations(new TransportOperation(message, new UnicastAddressTag(ENDPOINTNAME)));

           
            await messageDispatcher.Dispatch(transportOperations, new TransportTransaction(), new ContextBag());

            var receivedMessage = ReceiveMessage().Result;

            Assert.AreEqual(message.MessageId, receivedMessage.MessageId);


        }

        [OneTimeTearDown]
        public void TearDownn()
        {
            base.TearDown();
        }
    }*/
}

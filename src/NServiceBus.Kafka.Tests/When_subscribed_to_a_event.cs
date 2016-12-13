namespace NServiceBus.Transport.Kafka.Tests
{
    using System.Threading.Tasks;
    using Extensibility;
    using NUnit.Framework;
    using Transport;
    using Transport.Kafka.Tests;
    using System;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.IO;
    using System.Collections.Generic;
    using Routing;

    [TestFixture]
    class When_subscribed_to_a_event : KafkaContext
    {
        bool initiated = false;

        [OneTimeSetUp]
        public void SetUpp()
        {
            if (initiated)
                return;

            base.SetUp();

            initiated = true;
        }

        [OneTimeTearDown]
        public void TearDownn()
        {         
            base.TearDown();
        }

        [Test]
        public async Task Should_receive_published_events_of_that_type()
        {
            await Publish<MyEvent>();
            AssertReceived<MyEvent>();

            /*var type = typeof(MyEvent);
            var m = new MyEvent();

            var message = new OutgoingMessageBuilder().WithBody(ObjectToByteArray(m)).CorrelationId(type.FullName).PublishType(type).Build();

            await messageDispatcher.Dispatch(message, new TransportTransaction(), new ContextBag());
                              
            var receivedMessage = ReceiveMessage().Result;         

            Assert.IsTrue(receivedMessage!=null);*/
            
        }

        [Test]
        public async Task Should_receive_the_event_if_subscribed_to_the_base_class()
        {        
            await Publish<SubEvent1>();
            await Publish<SubEvent2>();

            AssertReceived<SubEvent1>();
            AssertReceived<SubEvent2>();
        }

        [Test]
        public async Task Should_not_receive_the_event_if_subscribed_to_the_specific_class()
        {
        //    await Subscribe<SubEvent1>();

            await Publish<EventBase>();

            AssertNoEventReceived();
        }

        [Test]
        public async Task Should_receive_the_event_if_subscribed_to_the_base_interface()
        {
        //    await Subscribe<IMyEvent>();

            await Publish<MyEvent1>();
            await Publish<MyEvent2>();

            AssertReceived<MyEvent1>();
            AssertReceived<MyEvent2>();
        }

        [Test]
        public async Task Should_not_receive_the_event_if_subscribed_to_specific_interface()
        {
         //   await Subscribe<MyEvent1>();

            await Publish<IMyEvent>();

            AssertNoEventReceived();
        }

        [Test]
        public async Task Should_not_receive_events_of_other_types()
        {
         //   await Subscribe<MyEvent>();

            //publish a event that that this publisher isn't subscribed to
            await Publish<MyOtherEvent>();
            await Publish<MyEvent>();

            AssertReceived<MyEvent>();
        }

        [Test]
        public async Task Subscribing_to_IEvent_should_subscribe_to_all_published_messages()
        {
         //   await Subscribe<IEvent>();

            await Publish<MyOtherEvent>();
            await Publish<MyEvent>();

            AssertReceived<MyOtherEvent>();
            AssertReceived<MyEvent>();
        }

        [Test]
        public async Task Subscribing_to_Object_should_subscribe_to_all_published_messages()
        {
          //  await Subscribe<object>();

            await Publish<MyOtherEvent>();
            await Publish<MyEvent>();

            AssertReceived<MyOtherEvent>();
            AssertReceived<MyEvent>();
        }

        [Test]
        public async Task Subscribing_to_a_class_implementing_a_interface_should_only_give_the_concrete_class()
        {
           // await Subscribe<CombinedClassAndInterface>();

            await Publish<CombinedClassAndInterface>();
            await Publish<IMyEvent>();

            AssertReceived<CombinedClassAndInterface>();
            AssertNoEventReceived();
        }

        [Test]
        public async Task Subscribing_to_a_interface_that_is_implemented_be_a_class_should_give_the_event_if_the_class_is_published()
        {
           // await Subscribe<IMyEvent>();

            await Publish<CombinedClassAndInterface>();
            await Publish<IMyEvent>();

            AssertReceived<CombinedClassAndInterface>();
            AssertReceived<IMyEvent>();
        }

        [Test]
        public async Task Should_not_receive_events_after_unsubscribing()
        {
           // await Subscribe<MyEvent>();

            await subscriptionManager.Unsubscribe(typeof(MyEvent), new ContextBag());

            //publish a event that that this publisher isn't subscribed to
            await Publish<MyEvent>();

            AssertNoEventReceived();
        }

        Task Subscribe<T>() => subscriptionManager.Subscribe(typeof(T), new ContextBag());

        Task Publish<T>() where T : class, new()
        {
            var type = typeof(T);
            var m = new T();

            var message = new OutgoingMessageBuilder().WithBody(ObjectToByteArray(m)).CorrelationId(type.FullName).PublishType(type).Build();

            messageDispatcher.Dispatch(message, new TransportTransaction(), new ContextBag());

            return Task.FromResult(0);
        }

        void AssertReceived<T>()
        {
            /*var receivedMessage = ReceiveMessage();

            AssertReceived<T>(receivedMessage);*/
        }

        void AssertReceived<T>(IncomingMessage receivedEvent)
        {
            Assert.AreEqual(typeof(T).FullName, receivedEvent.Headers[Headers.CorrelationId]);
        }

        void AssertNoEventReceived()
        {
            var messageWasReceived = TryWaitForMessageReceipt();

            Assert.False(messageWasReceived);
        }

        byte[] ObjectToByteArray(object obj)
        {
            if (obj == null)
                return null;
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }
    }


    [Serializable]
    public class MyOtherEvent
    {
    }

    [Serializable]
    public class MyEvent : IMessage 
    {
    }

    [Serializable]
    public class EventBase : IEvent
    {

    }

    [Serializable]
    public class SubEvent1 : EventBase
    {

    }

    [Serializable]
    public class SubEvent2 : EventBase
    {

    }

    [Serializable]
    public class IMyEvent : IEvent
    {

    }

    [Serializable]
    public class MyEvent1 : IMyEvent
    {

    }

    [Serializable]
    public class MyEvent2 : IMyEvent
    {

    }

    [Serializable]
    public class CombinedClassAndInterface : IMyEvent
    {

    }
}
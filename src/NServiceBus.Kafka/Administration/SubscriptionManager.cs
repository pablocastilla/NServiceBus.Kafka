using NServiceBus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Extensibility;
using NServiceBus.Transports.Kafka.Connection;
using RdKafka;
using System.Collections.Concurrent;

namespace NServiceBus.Transports.Kafka.Administration
{
    class SubscriptionManager : IManageSubscriptions
    {
        private readonly ConsumerFactory consumerFactory;

        public SubscriptionManager(ConsumerFactory consumerFactory)
        {
            this.consumerFactory = consumerFactory;
        }

        public Task Subscribe(Type eventType, ContextBag context)
        {           
           // var topics = GetTypeHierarchy( eventType);

            CreateSubscription(new HashSet<string>() { ExchangeName(eventType) });

            return Task.FromResult(0); 
        }

        public Task Unsubscribe(Type eventType, ContextBag context)
        {
            var consumer = consumerFactory.GetConsumer();

           // consumer.Unsubscribe();

            return Task.FromResult(0);
        }



        internal static HashSet<string> GetTypeHierarchy(Type type)
        {
            HashSet<string> topicsToSubscribe = new HashSet<string>();

            var typeToProcess = type;
            topicsToSubscribe.Add(ExchangeName(typeToProcess));
           
            var baseType = typeToProcess.BaseType;

            while (baseType != null)
            {
                /*if (type == typeof(Object))
                {
                    continue;
                }*/

                topicsToSubscribe.Add(ExchangeName(baseType));
                               
                typeToProcess = baseType;
                baseType = typeToProcess.BaseType;
            }

            foreach (var interfaceType in type.GetInterfaces())
            {              
                topicsToSubscribe.Add(ExchangeName(interfaceType));
            }

            return topicsToSubscribe;
           
        }

        void CreateSubscription(HashSet<string> topics)
        {
            var finalTopics = topics.Where(t => !typeTopologyConfiguredSet.ContainsKey(t));

            if (finalTopics.Count() == 0)
                return;

            var consumer = consumerFactory.GetConsumer();
            var subscriptionList = consumer.Subscription;

            foreach (var exchangeName in finalTopics)
            {
                if (!subscriptionList.Contains(exchangeName))
                    subscriptionList.Add(exchangeName);

                MarkTypeConfigured(exchangeName, consumer);

            }

            consumer.AddSubscriptionsBlocking(subscriptionList);
           
        }

       

        bool IsTypeTopologyKnownConfigured(string exchangeName) => typeTopologyConfiguredSet.ContainsKey(exchangeName);
        readonly ConcurrentDictionary<string, EventConsumer> typeTopologyConfiguredSet = new ConcurrentDictionary<string, EventConsumer>();
        static string ExchangeName(Type type) => type.Namespace + "." + type.Name;

        void MarkTypeConfigured(string exchangeName, EventConsumer consumer)
        {
            typeTopologyConfiguredSet[exchangeName] = consumer;
        }
    }
}
